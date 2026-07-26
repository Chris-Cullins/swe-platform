package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	"github.com/spf13/cobra"
)

var quotaKeys = []corev1.ResourceName{"requests.cpu", "requests.memory", "requests.storage", "persistentvolumeclaims", "pods", "secrets", "count/runs.swe.dev", "count/environments.swe.dev", "count/agentcredentialprofiles.swe.dev"}

type onboardOptions struct {
	namespace, systemNamespace, installation, repository, defaultTemplate string
	templates, quota                                                      []string
	adopt                                                                 bool
}

func newProjectCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "project", Short: "Onboard and fence project namespaces"}
	cmd.AddCommand(newProjectOnboardCommand(), newProjectOffboardCommand())
	return cmd
}

func newProjectOnboardCommand() *cobra.Command {
	o := &onboardOptions{}
	cmd := &cobra.Command{Use: "onboard PROJECT", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		o.namespace, _ = cmd.Flags().GetString("namespace")
		c, err := newKubeClients()
		if err != nil {
			return err
		}
		return onboardProject(cmd.Context(), c.Client, args[0], *o, cmd.OutOrStdout())
	}}
	cmd.Flags().StringVar(&o.systemNamespace, "system-namespace", "", "system namespace (required)")
	cmd.Flags().StringVar(&o.installation, "installation", "", "Installation name (required)")
	cmd.Flags().StringVar(&o.repository, "repository", "", "repository URL (required)")
	cmd.Flags().StringVar(&o.defaultTemplate, "default-template", "", "default template (required)")
	cmd.Flags().StringSliceVar(&o.templates, "template", nil, "catalog template (repeatable, required)")
	cmd.Flags().StringArrayVar(&o.quota, "quota-hard", nil, "quota KEY=QUANTITY (repeatable, all fixed keys required)")
	cmd.Flags().BoolVar(&o.adopt, "adopt", false, "adopt an existing unclaimed namespace/project")
	return cmd
}

func annotations(identity tenancy.InstallationIdentity, project string) map[string]string {
	return map[string]string{
		tenancy.InstallationNamespaceAnnotation: identity.Key.Namespace, tenancy.InstallationNameAnnotation: identity.Key.Name,
		tenancy.InstallationUIDAnnotation: string(identity.UID), tenancy.ProjectNameAnnotation: project,
	}
}
func exact(a map[string]string, key, value string) bool { return a != nil && a[key] == value }
func hasAuthorityAnnotation(a map[string]string) bool {
	for _, key := range []string{
		tenancy.InstallationNamespaceAnnotation,
		tenancy.InstallationNameAnnotation,
		tenancy.InstallationUIDAnnotation,
		tenancy.ProjectNameAnnotation,
		tenancy.ProjectUIDAnnotation,
		tenancy.LifecycleAnnotation,
		tenancy.LifecycleOperationAnnotation,
	} {
		if _, present := a[key]; present {
			return true
		}
	}
	return false
}

func parseOnboard(o onboardOptions) (corev1.ResourceList, error) {
	if strings.TrimSpace(o.systemNamespace) == "" || strings.TrimSpace(o.installation) == "" || strings.TrimSpace(o.repository) == "" || strings.TrimSpace(o.defaultTemplate) == "" || len(o.templates) == 0 {
		return nil, errors.New("--system-namespace, --installation, --repository, --default-template, and at least one --template are required")
	}
	seenT := map[string]bool{}
	for _, n := range o.templates {
		n = strings.TrimSpace(n)
		if n == "" || seenT[n] {
			return nil, fmt.Errorf("invalid or duplicate template %q", n)
		}
		seenT[n] = true
	}
	if !seenT[o.defaultTemplate] {
		return nil, errors.New("--template list must include --default-template")
	}
	allowed := map[corev1.ResourceName]bool{}
	for _, k := range quotaKeys {
		allowed[k] = true
	}
	hard := corev1.ResourceList{}
	for _, entry := range o.quota {
		p := strings.SplitN(entry, "=", 2)
		k := corev1.ResourceName(p[0])
		_, duplicate := hard[k]
		if len(p) != 2 || !allowed[k] || duplicate {
			return nil, fmt.Errorf("unknown, duplicate, or invalid quota %q", entry)
		}
		q, err := resource.ParseQuantity(p[1])
		if err != nil || q.Sign() <= 0 {
			return nil, fmt.Errorf("invalid quota %q", entry)
		}
		hard[k] = q
	}
	for _, k := range quotaKeys {
		if _, ok := hard[k]; !ok {
			return nil, fmt.Errorf("missing --quota-hard %s=QUANTITY", k)
		}
	}
	return hard, nil
}

func onboardProject(ctx context.Context, c client.Client, name string, o onboardOptions, out io.Writer) error {
	hard, err := parseOnboard(o)
	if err != nil {
		return err
	}
	key := types.NamespacedName{Namespace: o.systemNamespace, Name: o.installation}
	id, installation, err := tenancy.LoadInstallation(ctx, c, key)
	if err != nil {
		return err
	}
	for _, k := range []string{tenancy.OperatorServiceAccountAnnotation, tenancy.OperatorClusterRoleAnnotation} {
		if strings.TrimSpace(installation.Annotations[k]) == "" {
			return fmt.Errorf("Installation missing annotation %s", k)
		}
	}
	controlPlaneServiceAccount := strings.TrimSpace(installation.Annotations[tenancy.ControlPlaneServiceAccountAnnotation])
	controlPlaneClusterRole := strings.TrimSpace(installation.Annotations[tenancy.ControlPlaneClusterRoleAnnotation])
	if (controlPlaneServiceAccount == "") != (controlPlaneClusterRole == "") {
		return errors.New("Installation has an incomplete control-plane RBAC identity")
	}
	var ns corev1.Namespace
	err = c.Get(ctx, types.NamespacedName{Name: o.namespace}, &ns)
	if apierrors.IsNotFound(err) {
		ns = corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: o.namespace, Annotations: annotations(id, name)}}
		ns.Annotations[tenancy.LifecycleAnnotation] = string(tenancy.LifecycleFencing)
		ns.Annotations[tenancy.LifecycleOperationAnnotation] = tenancy.OperationOnboarding
		if err = c.Create(ctx, &ns); err != nil {
			return err
		}
		if err = c.Get(ctx, types.NamespacedName{Name: o.namespace}, &ns); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if ns.UID == "" || !ns.DeletionTimestamp.IsZero() {
			return errors.New("namespace has no stable live UID")
		}
		a := ns.Annotations
		claimed := hasAuthorityAnnotation(a)
		if !claimed {
			if !o.adopt {
				return errors.New("existing unclaimed Namespace requires --adopt")
			}
			a = merge(a, annotations(id, name))
			a[tenancy.LifecycleAnnotation] = string(tenancy.LifecycleFencing)
			a[tenancy.LifecycleOperationAnnotation] = tenancy.OperationOnboarding
			ns.Annotations = a
			if err = c.Update(ctx, &ns); err != nil {
				return err
			}
		} else if !claimMatches(a, id, name) {
			return errors.New("Namespace has a conflicting or stale claim")
		} else if lifecycle := tenancy.Lifecycle(a[tenancy.LifecycleAnnotation]); lifecycle != tenancy.LifecycleActive &&
			(lifecycle != tenancy.LifecycleFencing || a[tenancy.LifecycleOperationAnnotation] != tenancy.OperationOnboarding) {
			return fmt.Errorf("Namespace lifecycle %q cannot be reactivated by onboarding", lifecycle)
		}
	}
	nsUID := ns.UID
	if nsUID == "" {
		return errors.New("Namespace has no stable UID after creation")
	}
	expectedProjectUID := types.UID(strings.TrimSpace(ns.Annotations[tenancy.ProjectUIDAnnotation]))
	if tenancy.Lifecycle(ns.Annotations[tenancy.LifecycleAnnotation]) == tenancy.LifecycleActive && expectedProjectUID == "" {
		return errors.New("active Namespace claim has no immutable Project UID")
	}
	desiredSpec := platformv1alpha1.ProjectSpec{Repositories: []string{o.repository}, TemplateRef: o.defaultTemplate, ChangesWorkflow: platformv1alpha1.ChangesWorkflowBranchPR}
	var list platformv1alpha1.ProjectList
	if err = c.List(ctx, &list, client.InNamespace(o.namespace)); err != nil {
		return err
	}
	if len(list.Items) > 1 {
		return errors.New("Namespace contains multiple Projects")
	}
	var project platformv1alpha1.Project
	if len(list.Items) == 0 {
		if expectedProjectUID != "" {
			return errors.New("claimed Project UID is missing; refusing same-name replacement")
		}
		project = platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: o.namespace, Annotations: annotations(id, name)}, Spec: desiredSpec}
		if err = c.Create(ctx, &project); err != nil {
			return err
		}
		if err = c.Get(ctx, types.NamespacedName{Namespace: o.namespace, Name: name}, &project); err != nil {
			return err
		}
	} else {
		project = list.Items[0]
		if project.Name != name || !project.DeletionTimestamp.IsZero() || !reflect.DeepEqual(project.Spec, desiredSpec) {
			return errors.New("existing Project name/spec does not match")
		}
		if expectedProjectUID != "" && project.UID != expectedProjectUID {
			return errors.New("Project immutable UID does not match the Namespace claim")
		}
		if !claimMatches(project.Annotations, id, name) {
			if hasAuthorityAnnotation(project.Annotations) {
				return errors.New("Project has a conflicting or stale claim")
			}
			if !o.adopt {
				return errors.New("Project ownership does not match")
			}
			project.Annotations = merge(project.Annotations, annotations(id, name))
			if err = c.Update(ctx, &project); err != nil {
				return err
			}
		}
	}
	if project.UID == "" {
		return errors.New("Project has no stable live UID")
	}
	if value := project.Annotations[tenancy.ProjectUIDAnnotation]; value != "" && value != string(project.UID) {
		return errors.New("Project has a stale immutable UID annotation")
	}
	if project.Annotations == nil {
		project.Annotations = make(map[string]string)
	}
	if project.Annotations[tenancy.ProjectUIDAnnotation] != string(project.UID) {
		project.Annotations[tenancy.ProjectUIDAnnotation] = string(project.UID)
		if err = c.Update(ctx, &project); err != nil {
			return err
		}
	}
	if err = checkNamespace(ctx, c, o.namespace, nsUID, id, name); err != nil {
		return err
	}
	if err = patchNamespace(ctx, c, o.namespace, nsUID, id, name,
		map[string]string{
			tenancy.ProjectUIDAnnotation:         string(expectedProjectUID),
			tenancy.LifecycleAnnotation:          string(ns.Annotations[tenancy.LifecycleAnnotation]),
			tenancy.LifecycleOperationAnnotation: ns.Annotations[tenancy.LifecycleOperationAnnotation],
		},
		map[string]string{tenancy.ProjectUIDAnnotation: string(project.UID)}); err != nil {
		return err
	}
	verifier := &tenancy.Verifier{Reader: c, Installation: id, Mode: tenancy.ModeScoped}
	syncContext, syncClaim, err := (&tenancy.ReconcileScope{Verifier: verifier}).Begin(ctx, o.namespace, tenancy.LifecycleActive, tenancy.LifecycleFencing)
	if err != nil {
		return err
	}
	if syncClaim.NamespaceUID != nsUID || syncClaim.ProjectUID != project.UID || syncClaim.ProjectName != name {
		return errors.New("Namespace or Project identity changed before synchronization")
	}
	guarded := tenancy.GuardedClient{Client: c, Verifier: verifier}
	if err = syncTemplates(syncContext, guarded, id, project.UID, name, o.namespace, o.templates); err != nil {
		return err
	}
	if err = syncBaseline(syncContext, guarded, id, installation, project.UID, name, o.namespace, hard); err != nil {
		return err
	}
	if err = patchNamespace(ctx, c, o.namespace, nsUID, id, name,
		map[string]string{
			tenancy.ProjectUIDAnnotation:         string(project.UID),
			tenancy.LifecycleAnnotation:          ns.Annotations[tenancy.LifecycleAnnotation],
			tenancy.LifecycleOperationAnnotation: ns.Annotations[tenancy.LifecycleOperationAnnotation],
		},
		map[string]string{tenancy.LifecycleAnnotation: string(tenancy.LifecycleActive), tenancy.LifecycleOperationAnnotation: ""}); err != nil {
		return err
	}
	fmt.Fprintf(out, "Project %s onboarded. Add %s to tenancy.namespaces, then perform a controlled Helm upgrade/restart of the operator and control plane.\n", name, o.namespace)
	return nil
}

func merge(a, b map[string]string) map[string]string {
	r := map[string]string{}
	for k, v := range a {
		r[k] = v
	}
	for k, v := range b {
		r[k] = v
	}
	return r
}
func claimMatches(a map[string]string, id tenancy.InstallationIdentity, name string) bool {
	return exact(a, tenancy.InstallationNamespaceAnnotation, id.Key.Namespace) && exact(a, tenancy.InstallationNameAnnotation, id.Key.Name) && exact(a, tenancy.InstallationUIDAnnotation, string(id.UID)) && exact(a, tenancy.ProjectNameAnnotation, name)
}
func checkNamespace(ctx context.Context, c client.Client, n string, uid types.UID, id tenancy.InstallationIdentity, p string) error {
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: n}, &ns); err != nil {
		return err
	}
	if ns.UID != uid || !ns.DeletionTimestamp.IsZero() || !claimMatches(ns.Annotations, id, p) {
		return errors.New("Namespace identity or claim changed")
	}
	return nil
}
func patchNamespace(ctx context.Context, c client.Client, n string, uid types.UID, id tenancy.InstallationIdentity, project string, required, updates map[string]string) error {
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: n}, &ns); err != nil {
		return err
	}
	if ns.UID != uid || !ns.DeletionTimestamp.IsZero() || !claimMatches(ns.Annotations, id, project) {
		return errors.New("Namespace identity or claim changed")
	}
	for key, value := range required {
		if strings.TrimSpace(ns.Annotations[key]) != value {
			return fmt.Errorf("Namespace annotation %s changed", key)
		}
	}
	ns.Annotations = merge(ns.Annotations, updates)
	for key, value := range updates {
		if value == "" {
			delete(ns.Annotations, key)
		}
	}
	return c.Update(ctx, &ns)
}

func syncTemplates(ctx context.Context, c client.Client, id tenancy.InstallationIdentity, puid types.UID, p, target string, names []string) error {
	var sources platformv1alpha1.EnvironmentTemplateList
	if err := c.List(ctx, &sources, client.InNamespace(id.Key.Namespace)); err != nil {
		return err
	}
	by := map[string]*platformv1alpha1.EnvironmentTemplate{}
	for i := range sources.Items {
		s := &sources.Items[i]
		a := s.Annotations
		if tenancy.IsCatalogSource(s) && s.UID != "" && exact(a, tenancy.InstallationNamespaceAnnotation, id.Key.Namespace) && exact(a, tenancy.InstallationNameAnnotation, id.Key.Name) && exact(a, tenancy.InstallationUIDAnnotation, string(id.UID)) && a[tenancy.CatalogNameAnnotation] != "" && a[tenancy.CatalogRevisionAnnotation] != "" {
			if by[a[tenancy.CatalogNameAnnotation]] != nil {
				return fmt.Errorf("duplicate catalog source %s", a[tenancy.CatalogNameAnnotation])
			}
			by[a[tenancy.CatalogNameAnnotation]] = s
		}
	}
	for _, n := range names {
		s := by[n]
		if s == nil {
			return fmt.Errorf("catalog template %q not found", n)
		}
		managed := annotations(id, p)
		managed[tenancy.ProjectUIDAnnotation] = string(puid)
		managed[tenancy.CatalogNameAnnotation] = n
		managed[tenancy.CatalogRevisionAnnotation] = s.Annotations[tenancy.CatalogRevisionAnnotation]
		managed[tenancy.CatalogSourceUIDAnnotation] = string(s.UID)
		var dst platformv1alpha1.EnvironmentTemplate
		err := c.Get(ctx, types.NamespacedName{Namespace: target, Name: n}, &dst)
		if apierrors.IsNotFound(err) {
			dst = platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: target, Annotations: managed}, Spec: s.Spec}
			if err = c.Create(ctx, &dst); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			a := dst.Annotations
			if !claimMatches(a, id, p) || !exact(a, tenancy.ProjectUIDAnnotation, string(puid)) || !exact(a, tenancy.CatalogNameAnnotation, n) || strings.TrimSpace(a[tenancy.CatalogSourceUIDAnnotation]) == "" {
				return fmt.Errorf("Template %q ownership/source collision", n)
			}
			dst.Spec = s.Spec
			dst.Annotations = merge(a, managed)
			if err = c.Update(ctx, &dst); err != nil {
				return err
			}
		}
	}
	return nil
}

func syncBaseline(ctx context.Context, c client.Client, id tenancy.InstallationIdentity, ins *platformv1alpha1.Installation, puid types.UID, project, ns string, hard corev1.ResourceList) error {
	a := annotations(id, project)
	a[tenancy.ProjectUIDAnnotation] = string(puid)
	a[tenancy.BaselineVersionAnnotation] = tenancy.BaselineVersion
	objects := []client.Object{&corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: tenancy.BaselineResourceQuota, Namespace: ns}, Spec: corev1.ResourceQuotaSpec{Hard: hard}}, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: tenancy.EnvironmentServiceAccount, Namespace: ns}, AutomountServiceAccountToken: ptr(false)}, roleBinding(ns, tenancy.OperatorRoleBinding, ins.Annotations[tenancy.OperatorClusterRoleAnnotation], id.Key.Namespace, ins.Annotations[tenancy.OperatorServiceAccountAnnotation]), &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: tenancy.BaselineIngressPolicy, Namespace: ns}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}}}}
	if ins.Annotations[tenancy.ControlPlaneServiceAccountAnnotation] != "" {
		objects = append(objects, roleBinding(ns, tenancy.ControlPlaneRoleBinding, ins.Annotations[tenancy.ControlPlaneClusterRoleAnnotation], id.Key.Namespace, ins.Annotations[tenancy.ControlPlaneServiceAccountAnnotation]))
	}
	for _, o := range objects {
		o.SetAnnotations(a)
		cur := o.DeepCopyObject().(client.Object)
		err := c.Get(ctx, client.ObjectKeyFromObject(o), cur)
		if apierrors.IsNotFound(err) {
			if err = c.Create(ctx, o); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			ca := cur.GetAnnotations()
			if !exact(ca, tenancy.InstallationUIDAnnotation, string(id.UID)) || !exact(ca, tenancy.ProjectUIDAnnotation, string(puid)) {
				return fmt.Errorf("foreign baseline collision %s", o.GetName())
			}
			o.SetResourceVersion(cur.GetResourceVersion())
			o.SetUID(cur.GetUID())
			o.SetAnnotations(merge(ca, a))
			if err = c.Update(ctx, o); err != nil {
				return err
			}
		}
	}
	return nil
}
func ptr[T any](v T) *T { return &v }
func roleBinding(ns, name, role, sans, sa string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: sa, Namespace: sans}}}
}

func newProjectOffboardCommand() *cobra.Command {
	var system, installation string
	var timeout time.Duration
	cmd := &cobra.Command{Use: "offboard PROJECT", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ns, _ := cmd.Flags().GetString("namespace")
		c, e := newKubeClients()
		if e != nil {
			return e
		}
		return offboardProject(cmd.Context(), c.Client, args[0], ns, system, installation, timeout, cmd.OutOrStdout())
	}}
	cmd.Flags().StringVar(&system, "system-namespace", "", "system namespace (required)")
	cmd.Flags().StringVar(&installation, "installation", "", "Installation name (required)")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "wait timeout (required)")
	return cmd
}
func offboardProject(ctx context.Context, c client.Client, p, ns, system, installation string, timeout time.Duration, out io.Writer) error {
	if system == "" || installation == "" || timeout <= 0 {
		return errors.New("--system-namespace, --installation, and positive --timeout are required")
	}
	id, _, err := tenancy.LoadInstallation(ctx, c, types.NamespacedName{Namespace: system, Name: installation})
	if err != nil {
		return err
	}
	var n corev1.Namespace
	if err = c.Get(ctx, types.NamespacedName{Name: ns}, &n); err != nil {
		return err
	}
	if n.UID == "" || !claimMatches(n.Annotations, id, p) {
		return errors.New("Namespace claim mismatch")
	}
	verifier := &tenancy.Verifier{Reader: c, Installation: id, Mode: tenancy.ModeScoped}
	claim, err := verifier.VerifyNamespace(ctx, ns)
	if err != nil || claim.NamespaceUID != n.UID || claim.ProjectName != p {
		if err != nil {
			return err
		}
		return errors.New("Namespace Project identity mismatch")
	}
	uid := n.UID
	l := tenancy.Lifecycle(n.Annotations[tenancy.LifecycleAnnotation])
	if l == tenancy.LifecycleFenced {
		fmt.Fprintln(out, "Project already fenced; retain/archive resources, remove namespace from tenancy.namespaces, then perform a controlled Helm upgrade/restart of the operator and control plane.")
		return nil
	}
	if l != tenancy.LifecycleActive && l != tenancy.LifecycleFencing {
		return errors.New("Namespace lifecycle is not active/fencing/fenced")
	}
	if l == tenancy.LifecycleActive {
		if err = patchNamespace(ctx, c, ns, uid, id, p,
			map[string]string{
				tenancy.ProjectUIDAnnotation:         string(claim.ProjectUID),
				tenancy.LifecycleAnnotation:          string(tenancy.LifecycleActive),
				tenancy.LifecycleOperationAnnotation: "",
			},
			map[string]string{tenancy.LifecycleAnnotation: string(tenancy.LifecycleFencing), tenancy.LifecycleOperationAnnotation: tenancy.OperationOffboarding}); err != nil {
			return err
		}
	} else if n.Annotations[tenancy.LifecycleOperationAnnotation] != tenancy.OperationOffboarding {
		return errors.New("Namespace is fencing for a different operation")
	}
	deadline := time.Now().Add(timeout)
	for {
		if err = checkNamespace(ctx, c, ns, uid, id, p); err != nil {
			return err
		}
		iterationContext, currentClaim, verifyErr := (&tenancy.ReconcileScope{Verifier: verifier}).Begin(ctx, ns, tenancy.LifecycleFencing)
		if verifyErr != nil || currentClaim.NamespaceUID != uid || currentClaim.ProjectUID != claim.ProjectUID || currentClaim.Lifecycle != tenancy.LifecycleFencing {
			if verifyErr != nil {
				return verifyErr
			}
			return errors.New("Namespace claim changed while offboarding")
		}
		var runs platformv1alpha1.RunList
		var envs platformv1alpha1.EnvironmentList
		var pods corev1.PodList
		if err = c.List(ctx, &runs, client.InNamespace(ns)); err != nil {
			return err
		}
		if err = c.List(ctx, &envs, client.InNamespace(ns)); err != nil {
			return err
		}
		if err = c.List(ctx, &pods, client.InNamespace(ns)); err != nil {
			return err
		}
		guarded := tenancy.GuardedClient{Client: c, Verifier: verifier}
		if err = ensureOffboardingIntents(iterationContext, guarded, runs.Items, envs.Items); err != nil {
			return err
		}
		done := true
		for _, r := range runs.Items {
			if !runTerminal(r.Status.State) || !r.DeletionTimestamp.IsZero() {
				done = false
			}
		}
		for _, e := range envs.Items {
			if !e.DeletionTimestamp.IsZero() || !e.Status.Lifecycle.Suspended || e.Status.PodName != "" || e.Status.Endpoints.Sandboxd != "" || e.Status.Endpoints.Terminal != "" {
				done = false
			}
		}
		for _, pod := range pods.Items {
			for _, owner := range pod.OwnerReferences {
				if owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "Environment" {
					done = false
				}
			}
		}
		if done {
			if err = patchNamespace(ctx, c, ns, uid, id, p,
				map[string]string{
					tenancy.ProjectUIDAnnotation:         string(claim.ProjectUID),
					tenancy.LifecycleAnnotation:          string(tenancy.LifecycleFencing),
					tenancy.LifecycleOperationAnnotation: tenancy.OperationOffboarding,
				},
				map[string]string{tenancy.LifecycleAnnotation: string(tenancy.LifecycleFenced), tenancy.LifecycleOperationAnnotation: ""}); err != nil {
				return err
			}
			fmt.Fprintln(out, "Project fenced; retain/archive resources, remove namespace from tenancy.namespaces, then perform a controlled Helm upgrade/restart of the operator and control plane.")
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for Runs and Environments; Namespace remains fencing")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func ensureOffboardingIntents(ctx context.Context, c client.Client, runs []platformv1alpha1.Run, environments []platformv1alpha1.Environment) error {
	for i := range runs {
		run := &runs[i]
		if !run.DeletionTimestamp.IsZero() || run.Spec.Cancel {
			continue
		}
		before := run.DeepCopy()
		run.Spec.Cancel = true
		if err := c.Patch(ctx, run, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("publish cancellation for Run %q: %w", run.Name, err)
		}
	}
	for i := range environments {
		environment := &environments[i]
		if !environment.DeletionTimestamp.IsZero() || environment.Spec.Lifecycle.Hold != nil && environment.Spec.Lifecycle.Hold.Enabled {
			continue
		}
		before := environment.DeepCopy()
		revision := int64(1)
		if environment.Spec.Lifecycle.Hold != nil {
			revision = environment.Spec.Lifecycle.Hold.Revision + 1
		}
		environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true, Revision: revision}
		if err := c.Patch(ctx, environment, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("publish hold for Environment %q: %w", environment.Name, err)
		}
	}
	return nil
}

func runTerminal(s platformv1alpha1.RunState) bool {
	return s == platformv1alpha1.RunStateSucceeded || s == platformv1alpha1.RunStateFailed || s == platformv1alpha1.RunStateCancelled
}
