package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

const (
	tuiPollInterval      = 4 * time.Second
	maxTranscriptEntries = 500
	maxTranscriptBytes   = 4096
)

type tuiMode uint8

const (
	tuiList tuiMode = iota
	tuiDetail
	tuiCreate
	tuiConfirmCancel
)

type tuiModel struct {
	ctx       context.Context
	client    *controlplaneclient.Client
	namespace string
	send      func(tea.Msg)

	mode             tuiMode
	width            int
	height           int
	loading          bool
	listInFlight     bool
	detailInFlight   bool
	envInFlight      bool
	mutationInFlight bool
	mutationID       uint64
	status           string
	err              string
	runs             []controlplane.Run
	cursor           int
	run              *controlplane.Run
	env              *controlplane.Environment

	streamCancel        context.CancelFunc
	streamID            runIdentity
	streamGeneration    uint64
	streamCursor        string
	streamRecoveryCount int
	streamBlocked       bool
	transcript          []string

	fields    []textinput.Model
	prompt    textarea.Model
	formFocus int
}

type runIdentity struct {
	namespace string
	name      string
	uid       string
}

type runsLoadedMsg struct {
	runs []controlplane.Run
	err  error
}

type runLoadedMsg struct {
	name string
	run  controlplane.Run
	err  error
}

type environmentLoadedMsg struct {
	identity    runIdentity
	environment controlplane.Environment
	err         error
}

type transcriptMsg struct {
	identity   runIdentity
	generation uint64
	event      controlplaneclient.SSEEvent
}

type transcriptDoneMsg struct {
	identity   runIdentity
	generation uint64
	err        error
}

type mutationDoneMsg struct {
	run    controlplane.Run
	create bool
	id     uint64
	err    error
}

type attachDoneMsg struct{ err error }
type pollMsg time.Time

func newTUICommand() *cobra.Command {
	var controlPlaneURL, token string
	var check bool
	cmd := &cobra.Command{
		Use:   "tui",
		Short: "Open the terminal operations console",
		Long: `Open a keyboard-first, agent-neutral Run dashboard for one namespace.
All Run, transcript, Environment, and terminal operations use the authenticated
control-plane API. Credentials are used only for requests and are never shown.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			client, err := controlplaneclient.New(controlPlaneURL, token, nil)
			if err != nil {
				return err
			}
			if check {
				runs, err := client.ListRuns(cmd.Context(), namespace)
				if err != nil {
					return fmt.Errorf("check terminal console API: %w", err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "terminal console API ready for namespace %s (%d Runs)\n", safeText(namespace), len(runs))
				return nil
			}
			return runTUI(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), client, namespace)
		},
	}
	cmd.Flags().StringVar(&controlPlaneURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	cmd.Flags().BoolVar(&check, "check", false, "Validate authenticated Run API access without opening the interactive console")
	return cmd
}

func runTUI(ctx context.Context, input io.Reader, output io.Writer, client *controlplaneclient.Client, namespace string) error {
	tuiCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	model := newTUIModel(tuiCtx, client, namespace)
	program := tea.NewProgram(model, tea.WithContext(tuiCtx), tea.WithInput(input), tea.WithOutput(output), tea.WithAltScreen())
	model.send = program.Send
	_, err := program.Run()
	cancel()
	model.stopStream()
	if err != nil {
		return fmt.Errorf("run terminal console: %w", err)
	}
	return nil
}

func newTUIModel(ctx context.Context, client *controlplaneclient.Client, namespace string) *tuiModel {
	labels := []string{"Run name", "Agent", "Environment", "Project", "Template", "Credential profile"}
	fields := make([]textinput.Model, len(labels))
	for i, label := range labels {
		fields[i] = textinput.New()
		fields[i].Prompt = label + ": "
		fields[i].CharLimit = 253
	}
	fields[0].Placeholder = "stable idempotency key"
	fields[1].Placeholder = "agent adapter name"
	prompt := textarea.New()
	prompt.Placeholder = "Task prompt"
	prompt.SetHeight(5)
	prompt.CharLimit = 1 << 20
	return &tuiModel{ctx: ctx, client: client, namespace: namespace, loading: true, fields: fields, prompt: prompt}
}

func (m *tuiModel) Init() tea.Cmd {
	return tea.Batch(m.loadRuns(), pollAfter())
}

func (m *tuiModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.resizeInputs()
		return m, nil
	case pollMsg:
		commands := []tea.Cmd{pollAfter()}
		if !m.listInFlight {
			commands = append(commands, m.loadRuns())
		}
		if m.mode == tuiDetail && m.run != nil && !m.detailInFlight {
			commands = append(commands, m.loadRun(m.run.Name))
		}
		return m, tea.Batch(commands...)
	case runsLoadedMsg:
		m.listInFlight = false
		m.loading = false
		if msg.err != nil {
			m.err = safeError(msg.err)
			return m, nil
		}
		m.err = ""
		sort.SliceStable(msg.runs, func(i, j int) bool { return msg.runs[i].CreatedAt.After(msg.runs[j].CreatedAt) })
		selected := m.selectedRunName()
		m.runs = msg.runs
		m.cursor = indexRun(m.runs, selected)
		if m.cursor < 0 {
			m.cursor = 0
		}
		return m, nil
	case runLoadedMsg:
		m.detailInFlight = false
		if m.mode != tuiDetail || m.run == nil || msg.name != m.run.Name {
			return m, nil
		}
		if msg.err != nil {
			m.err = safeError(msg.err)
			return m, nil
		}
		m.err = ""
		identity := runIdentity{namespace: m.namespace, name: msg.run.Name, uid: msg.run.UID}
		if identity != m.streamID {
			m.stopStream()
			m.transcript = nil
			m.streamCursor = ""
			m.streamRecoveryCount = 0
			m.streamBlocked = false
		}
		m.run = &msg.run
		commands := []tea.Cmd{}
		if msg.run.Environment != nil {
			if !m.envInFlight {
				commands = append(commands, m.loadEnvironment(identity, msg.run.Environment.Name))
			}
		} else {
			m.env = nil
		}
		if m.streamCancel == nil && identity.uid != "" && !m.streamBlocked {
			commands = append(commands, m.startTranscript(identity))
		}
		return m, tea.Batch(commands...)
	case environmentLoadedMsg:
		m.envInFlight = false
		if msg.identity != m.currentIdentity() {
			return m, nil
		}
		if msg.err != nil {
			m.env = nil
			m.err = safeError(msg.err)
		} else {
			m.env = &msg.environment
		}
		return m, nil
	case transcriptMsg:
		if msg.identity != m.currentIdentity() || msg.identity != m.streamID || msg.generation != m.streamGeneration {
			return m, nil
		}
		if msg.event.ID != "" && msg.event.ID == m.streamCursor {
			return m, nil
		}
		if msg.event.ID != "" {
			m.streamCursor = msg.event.ID
		}
		m.appendTranscript(formatTranscriptEvent(msg.event))
		return m, nil
	case transcriptDoneMsg:
		if msg.identity != m.streamID || msg.generation != m.streamGeneration {
			return m, nil
		}
		m.streamCancel = nil
		if recovery, ok := controlplaneclient.TranscriptCursorRecovery(msg.err); ok {
			if recovery.ResumeAfter == m.streamCursor || m.streamRecoveryCount >= 1 {
				m.streamBlocked = true
				m.err = "transcript cursor could not recover safely; press r to retry"
				return m, nil
			}
			m.appendTranscript("! TRANSCRIPT GAP: " + boundedSafeJSON(recovery.Available))
			m.streamCursor = recovery.ResumeAfter
			m.streamRecoveryCount++
			if m.mode == tuiDetail && msg.identity == m.currentIdentity() {
				return m, m.startTranscript(msg.identity)
			}
			return m, nil
		}
		if msg.err != nil && m.ctx.Err() == nil && m.mode == tuiDetail {
			m.err = "transcript: " + safeError(msg.err)
		}
		return m, nil
	case mutationDoneMsg:
		if !m.mutationInFlight || msg.id != m.mutationID {
			return m, nil
		}
		m.mutationInFlight = false
		m.loading = false
		if msg.err != nil {
			m.err = safeError(msg.err)
			return m, nil
		}
		m.err = ""
		m.status = "cancellation requested"
		if msg.create {
			m.status = "Run created"
			m.resetForm()
		}
		m.mode = tuiDetail
		m.run = &msg.run
		return m, tea.Batch(m.loadRuns(), m.loadRun(msg.run.Name))
	case attachDoneMsg:
		if msg.err != nil {
			m.err = safeError(msg.err)
		} else {
			m.status = "terminal detached"
		}
		if m.run != nil {
			return m, m.loadRun(m.run.Name)
		}
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *tuiModel) handleKey(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.mode == tuiCreate {
		return m.handleFormKey(key)
	}
	if m.mode == tuiConfirmCancel {
		switch key.String() {
		case "y", "Y":
			m.mode = tuiDetail
			m.loading = true
			m.mutationInFlight = true
			m.mutationID++
			return m, m.cancelRun(m.mutationID)
		case "n", "N", "esc":
			m.mode = tuiDetail
		}
		return m, nil
	}
	switch key.String() {
	case "ctrl+c", "q":
		m.stopStream()
		return m, tea.Quit
	case "c":
		m.stopStream()
		m.mode = tuiCreate
		m.err, m.status = "", ""
		m.formFocus = 0
		m.focusForm()
		return m, nil
	case "r":
		m.loading = true
		if m.mode == tuiDetail && m.run != nil {
			m.streamBlocked = false
			m.streamRecoveryCount = 0
			return m, tea.Batch(m.loadRuns(), m.loadRun(m.run.Name))
		}
		return m, m.loadRuns()
	case "esc", "backspace":
		if m.mode == tuiDetail {
			m.stopStream()
			m.mode = tuiList
			m.run, m.env = nil, nil
		}
		return m, nil
	}
	if m.mode == tuiList {
		switch key.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor+1 < len(m.runs) {
				m.cursor++
			}
		case "enter":
			if len(m.runs) != 0 {
				run := m.runs[m.cursor]
				m.run = &run
				m.mode = tuiDetail
				m.loading = true
				m.err, m.status = "", ""
				m.streamBlocked = false
				m.streamRecoveryCount = 0
				return m, m.loadRun(run.Name)
			}
		}
		return m, nil
	}
	if m.mode == tuiDetail && m.run != nil {
		switch key.String() {
		case "x":
			if !isTerminalRun(m.run.State) && !m.run.CancelRequested {
				m.mode = tuiConfirmCancel
			}
		case "t":
			if m.run.Environment != nil {
				command := &terminalExec{ctx: m.ctx, client: m.client, namespace: m.namespace, environment: m.run.Environment.Name}
				return m, tea.Exec(command, func(err error) tea.Msg { return attachDoneMsg{err: err} })
			}
		}
	}
	return m, nil
}

func (m *tuiModel) handleFormKey(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.mutationInFlight {
		if key.String() == "ctrl+c" {
			return m, tea.Quit
		}
		return m, nil
	}
	switch key.String() {
	case "ctrl+c":
		m.resetForm()
		return m, tea.Quit
	case "esc":
		m.resetForm()
		m.mode = tuiList
		return m, nil
	case "ctrl+s":
		request := m.createRequest()
		m.loading = true
		m.err = ""
		m.mutationInFlight = true
		m.mutationID++
		return m, m.createRun(request, m.mutationID)
	case "tab", "shift+tab":
		direction := 1
		if key.String() == "shift+tab" {
			direction = -1
		}
		m.formFocus = (m.formFocus + direction + len(m.fields) + 1) % (len(m.fields) + 1)
		m.focusForm()
		return m, nil
	}
	var command tea.Cmd
	if m.formFocus == len(m.fields) {
		m.prompt, command = m.prompt.Update(key)
	} else {
		m.fields[m.formFocus], command = m.fields[m.formFocus].Update(key)
	}
	return m, command
}

func (m *tuiModel) View() string {
	width := m.width
	if width < 20 {
		width = 20
	}
	var body strings.Builder
	fmt.Fprintf(&body, "swe operations — namespace %s\n", truncate(safeText(m.namespace), width-29))
	if m.loading {
		body.WriteString("loading…\n")
	}
	if m.err != "" {
		fmt.Fprintf(&body, "error: %s\n", truncate(m.err, width-7))
	} else if m.status != "" {
		fmt.Fprintf(&body, "%s\n", truncate(m.status, width))
	}
	switch m.mode {
	case tuiCreate:
		body.WriteString("Create Run (agent is free-form)\n")
		if m.mutationInFlight {
			body.WriteString("creating… (Ctrl-C cancels and quits)\n")
		}
		for i := range m.fields {
			body.WriteString(m.fields[i].View())
			body.WriteByte('\n')
		}
		body.WriteString("Prompt:\n")
		body.WriteString(m.prompt.View())
		body.WriteString("\nTab fields • Ctrl-S submit • Esc cancel")
	case tuiConfirmCancel:
		fmt.Fprintf(&body, "Request cancellation of Run %s? [y/N]", safeText(m.run.Name))
	case tuiDetail:
		m.renderDetail(&body, width)
	default:
		m.renderList(&body, width)
	}
	return body.String()
}

func (m *tuiModel) renderList(body *strings.Builder, width int) {
	if len(m.runs) == 0 && !m.loading {
		body.WriteString("No Runs in this namespace.\n")
	}
	for i, run := range m.runs {
		marker := "  "
		if i == m.cursor {
			marker = "> "
		}
		environment := "—"
		if run.Environment != nil {
			environment = run.Environment.Name
		}
		age := shortAge(time.Since(run.CreatedAt))
		line := fmt.Sprintf("%s%-22s %-14s %-14s %-8s %-20s %s", marker, safeText(run.Name), safeText(run.State), safeText(run.Intent.Agent), age, safeText(environment), safeText(promptPreview(run.Intent.Prompt)))
		body.WriteString(truncate(line, width))
		body.WriteByte('\n')
	}
	body.WriteString("↑/↓ select • Enter details • c create • r refresh • q quit")
}

func (m *tuiModel) renderDetail(body *strings.Builder, width int) {
	if m.run == nil {
		body.WriteString("Run unavailable")
		return
	}
	run := m.run
	fmt.Fprintf(body, "Run: %s\nState: %s", truncate(safeText(run.Name), width-5), safeText(run.State))
	if run.CancelRequested {
		body.WriteString(" (cancellation requested)")
	}
	fmt.Fprintf(body, "\nAgent: %s\nPrompt: %s\n", safeText(run.Intent.Agent), truncate(safeText(run.Intent.Prompt), width-8))
	selector, selectorValue := selectedSelector(run.Intent.Selector)
	fmt.Fprintf(body, "Selector: %s=%s\nUsage: CPU %ds • tokens %d in / %d out\n", selector, safeText(selectorValue), run.Usage.CPUSeconds, run.Usage.TokensIn, run.Usage.TokensOut)
	if run.Environment == nil {
		body.WriteString("Environment: not allocated\n")
	} else if m.env == nil {
		fmt.Fprintf(body, "Environment: %s (loading)\n", safeText(run.Environment.Name))
	} else {
		fmt.Fprintf(body, "Environment: %s • phase %s • ready %t • paused %t\n", safeText(m.env.Name), safeText(m.env.Phase), m.env.Ready, m.env.Paused)
	}
	body.WriteString("Transcript (opaque adapter events):\n")
	available := m.height - 13
	if available < 3 {
		available = 3
	}
	start := len(m.transcript) - available
	if start < 0 {
		start = 0
	}
	for _, line := range m.transcript[start:] {
		body.WriteString(truncate(line, width))
		body.WriteByte('\n')
	}
	body.WriteString("Esc back • x cancel • t attach • c create • r refresh • q quit")
}

func (m *tuiModel) loadRuns() tea.Cmd {
	if m.listInFlight {
		return nil
	}
	m.listInFlight = true
	return func() tea.Msg {
		runs, err := m.client.ListRuns(m.ctx, m.namespace)
		return runsLoadedMsg{runs: runs, err: err}
	}
}

func (m *tuiModel) loadRun(name string) tea.Cmd {
	if m.detailInFlight {
		return nil
	}
	m.detailInFlight = true
	return func() tea.Msg {
		run, err := m.client.GetRun(m.ctx, m.namespace, name)
		return runLoadedMsg{name: name, run: run, err: err}
	}
}

func (m *tuiModel) loadEnvironment(identity runIdentity, name string) tea.Cmd {
	if m.envInFlight {
		return nil
	}
	m.envInFlight = true
	return func() tea.Msg {
		environment, err := m.client.GetEnvironment(m.ctx, m.namespace, name)
		return environmentLoadedMsg{identity: identity, environment: environment, err: err}
	}
}

func (m *tuiModel) startTranscript(identity runIdentity) tea.Cmd {
	ctx, cancel := context.WithCancel(m.ctx)
	m.streamCancel = cancel
	m.streamID = identity
	m.streamGeneration++
	generation := m.streamGeneration
	cursor := m.streamCursor
	send := m.send
	endpoint := m.client.Endpoint("api", "v1", "namespaces", identity.namespace, "runs", identity.name, "transcript")
	return func() tea.Msg {
		checkIdentity := func(checkCtx context.Context) error {
			run, err := m.client.GetRun(checkCtx, identity.namespace, identity.name)
			if err != nil {
				return fmt.Errorf("verify Run identity before transcript connection: %w", err)
			}
			if run.UID != identity.uid {
				return fmt.Errorf("Run %s was replaced; refreshing transcript identity", safeText(identity.name))
			}
			return nil
		}
		err := m.client.StreamSSEWithReconnectCheck(ctx, endpoint, cursor, checkIdentity, func(event controlplaneclient.SSEEvent) error {
			if send != nil {
				send(transcriptMsg{identity: identity, generation: generation, event: event})
			}
			return nil
		})
		return transcriptDoneMsg{identity: identity, generation: generation, err: err}
	}
}

func (m *tuiModel) stopStream() {
	if m.streamCancel != nil {
		m.streamCancel()
		m.streamCancel = nil
	}
	m.streamGeneration++
	m.streamID = runIdentity{}
}

func (m *tuiModel) createRun(request controlplane.CreateRunRequest, id uint64) tea.Cmd {
	return func() tea.Msg {
		run, err := m.client.CreateRun(m.ctx, m.namespace, request)
		return mutationDoneMsg{run: run, create: true, id: id, err: err}
	}
}

func (m *tuiModel) cancelRun(id uint64) tea.Cmd {
	name := m.run.Name
	return func() tea.Msg {
		run, err := m.client.CancelRun(m.ctx, m.namespace, name)
		return mutationDoneMsg{run: run, id: id, err: err}
	}
}

func (m *tuiModel) createRequest() controlplane.CreateRunRequest {
	return controlplane.CreateRunRequest{
		Name:  strings.TrimSpace(m.fields[0].Value()),
		Agent: strings.TrimSpace(m.fields[1].Value()),
		Selector: controlplane.RunSelector{
			Environment: strings.TrimSpace(m.fields[2].Value()),
			Project:     strings.TrimSpace(m.fields[3].Value()),
			Template:    strings.TrimSpace(m.fields[4].Value()),
		},
		CredentialProfile: strings.TrimSpace(m.fields[5].Value()),
		Prompt:            m.prompt.Value(),
	}
}

func (m *tuiModel) resetForm() {
	for i := range m.fields {
		m.fields[i].SetValue("")
		m.fields[i].Blur()
	}
	m.prompt.Reset()
	m.prompt.Blur()
	m.formFocus = 0
}

func (m *tuiModel) focusForm() {
	for i := range m.fields {
		m.fields[i].Blur()
	}
	m.prompt.Blur()
	if m.formFocus == len(m.fields) {
		m.prompt.Focus()
	} else {
		m.fields[m.formFocus].Focus()
	}
}

func (m *tuiModel) resizeInputs() {
	width := m.width - 4
	if width < 16 {
		width = 16
	}
	for i := range m.fields {
		m.fields[i].Width = width
	}
	m.prompt.SetWidth(width)
}

func (m *tuiModel) currentIdentity() runIdentity {
	if m.run == nil {
		return runIdentity{}
	}
	return runIdentity{namespace: m.namespace, name: m.run.Name, uid: m.run.UID}
}

func (m *tuiModel) selectedRunName() string {
	if len(m.runs) == 0 || m.cursor < 0 || m.cursor >= len(m.runs) {
		return ""
	}
	return m.runs[m.cursor].Name
}

func (m *tuiModel) appendTranscript(line string) {
	m.transcript = append(m.transcript, line)
	if len(m.transcript) > maxTranscriptEntries {
		copy(m.transcript, m.transcript[len(m.transcript)-maxTranscriptEntries:])
		m.transcript = m.transcript[:maxTranscriptEntries]
	}
}

func pollAfter() tea.Cmd {
	return tea.Tick(tuiPollInterval, func(now time.Time) tea.Msg { return pollMsg(now) })
}

type terminalExec struct {
	ctx                    context.Context
	client                 *controlplaneclient.Client
	namespace, environment string
	stdin                  io.Reader
	stdout, stderr         io.Writer
}

func (c *terminalExec) SetStdin(reader io.Reader)  { c.stdin = reader }
func (c *terminalExec) SetStdout(writer io.Writer) { c.stdout = writer }
func (c *terminalExec) SetStderr(writer io.Writer) { c.stderr = writer }
func (c *terminalExec) Run() error {
	return attachTerminalWithClient(c.ctx, c.client, c.namespace, c.environment, c.stdin, c.stdout)
}

func formatTranscriptEvent(event controlplaneclient.SSEEvent) string {
	if event.Event == "transcript-gap" {
		return "! TRANSCRIPT GAP: " + boundedSafeJSON(event.Data)
	}
	var envelope struct {
		Source string          `json:"source"`
		Type   string          `json:"type"`
		Data   json.RawMessage `json:"data"`
	}
	if json.Unmarshal(event.Data, &envelope) != nil {
		return safeText(event.Event) + " " + boundedSafeJSON(event.Data)
	}
	return fmt.Sprintf("[%s/%s] %s", safeText(envelope.Source), safeText(envelope.Type), boundedSafeJSON(envelope.Data))
}

func boundedSafeJSON(value []byte) string {
	if len(value) > maxTranscriptBytes {
		value = value[:maxTranscriptBytes]
	}
	var compact bytes.Buffer
	if json.Compact(&compact, value) != nil {
		return safeText(string(value))
	}
	return safeText(compact.String())
}

func safeText(value string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || unicode.IsControl(r) || r == '\u007f' || r >= '\u0080' && r <= '\u009f' {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(value, "�"))
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	return safeText(err.Error())
}

func truncate(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if utf8.RuneCountInString(value) <= width {
		return value
	}
	if width == 1 {
		return "…"
	}
	return string([]rune(value)[:width-1]) + "…"
}

func promptPreview(value string) string {
	return truncate(strings.Join(strings.Fields(safeText(value)), " "), 48)
}

func shortAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	if age < time.Minute {
		return "<1m"
	}
	if age < time.Hour {
		return fmt.Sprintf("%dm", int(age.Minutes()))
	}
	if age < 24*time.Hour {
		return fmt.Sprintf("%dh", int(age.Hours()))
	}
	return fmt.Sprintf("%dd", int(age.Hours()/24))
}

func selectedSelector(selector controlplane.RunSelector) (string, string) {
	if selector.Environment != "" {
		return "environment", selector.Environment
	}
	if selector.Project != "" && selector.Template != "" {
		return "project/template", selector.Project + "/" + selector.Template
	}
	if selector.Project != "" {
		return "project", selector.Project
	}
	return "template", selector.Template
}

func isTerminalRun(state string) bool {
	return state == "Succeeded" || state == "Failed" || state == "Cancelled"
}

func indexRun(runs []controlplane.Run, name string) int {
	for i := range runs {
		if runs[i].Name == name {
			return i
		}
	}
	return -1
}
