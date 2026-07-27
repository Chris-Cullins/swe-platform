// Package serviceconfig parses repository-owned .swe/services.yaml files.
package serviceconfig

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// MaxInputBytes is the largest services file accepted by Parse.
	MaxInputBytes = 64 * 1024
	MaxArgs       = 64
	MaxArgBytes   = 4096
	MaxArgvBytes  = 16 * 1024
)

// Declaration is one validated service, independent of the platform CRDs.
type Declaration struct {
	Name string
	Argv []string
	Port *int32
}

// Parse strictly parses a version 1 services declaration. Nil and zero-length
// input represent an absent file and return an empty declaration set.
func Parse(input []byte) ([]Declaration, error) {
	if len(input) == 0 {
		return []Declaration{}, nil
	}
	if len(input) > MaxInputBytes {
		return nil, fmt.Errorf("services file exceeds %d bytes", MaxInputBytes)
	}
	if !utf8.Valid(input) {
		return nil, errors.New("services file is not valid UTF-8")
	}

	decoder := yaml.NewDecoder(bytes.NewReader(input))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("parse services file: %w", err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("services file contains multiple YAML documents")
		}
		return nil, fmt.Errorf("parse services file: %w", err)
	}
	if err := inspectNode(&document); err != nil {
		return nil, err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("services file must contain a mapping")
	}
	root := document.Content[0]
	var version, services *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		key, value := root.Content[i], root.Content[i+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return nil, errors.New("top-level field names must be strings")
		}
		switch key.Value {
		case "version":
			version = value
		case "services":
			services = value
		default:
			return nil, fmt.Errorf("unknown top-level field %q", key.Value)
		}
	}
	if version == nil || version.Kind != yaml.ScalarNode || version.Tag != "!!int" || version.Value != "1" {
		return nil, errors.New("version is required and must be the integer 1")
	}
	if services == nil || services.Kind != yaml.MappingNode {
		return nil, errors.New("services is required and must be a mapping")
	}
	result := make([]Declaration, 0, len(services.Content)/2)
	for i := 0; i < len(services.Content); i += 2 {
		declaration, err := parseService(services.Content[i], services.Content[i+1])
		if err != nil {
			return nil, err
		}
		result = append(result, declaration)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func parseService(key, value *yaml.Node) (Declaration, error) {
	if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
		return Declaration{}, errors.New("service names must be strings")
	}
	name := key.Value
	if len(name) > 32 || len(validation.IsDNS1123Label(name)) != 0 {
		return Declaration{}, fmt.Errorf("service name %q must be a DNS-1123 label of at most 32 bytes", name)
	}
	if value.Kind != yaml.MappingNode {
		return Declaration{}, fmt.Errorf("service %q must be a mapping", name)
	}
	var command, port *yaml.Node
	for i := 0; i < len(value.Content); i += 2 {
		field, fieldValue := value.Content[i], value.Content[i+1]
		if field.Kind != yaml.ScalarNode || field.Tag != "!!str" {
			return Declaration{}, fmt.Errorf("service %q field names must be strings", name)
		}
		switch field.Value {
		case "command":
			command = fieldValue
		case "port":
			port = fieldValue
		default:
			return Declaration{}, fmt.Errorf("service %q has unknown field %q", name, field.Value)
		}
	}
	if command == nil || command.Kind != yaml.SequenceNode || len(command.Content) == 0 {
		return Declaration{}, fmt.Errorf("service %q command is required and must be a non-empty sequence", name)
	}
	if len(command.Content) > MaxArgs {
		return Declaration{}, fmt.Errorf("service %q command has more than %d arguments", name, MaxArgs)
	}
	declaration := Declaration{Name: name, Argv: make([]string, len(command.Content))}
	total := 0
	for i, argument := range command.Content {
		if argument.Kind != yaml.ScalarNode || argument.Tag != "!!str" {
			return Declaration{}, fmt.Errorf("service %q command argument %d must be a string", name, i)
		}
		if !utf8.ValidString(argument.Value) || bytes.IndexByte([]byte(argument.Value), 0) >= 0 {
			return Declaration{}, fmt.Errorf("service %q command argument %d contains invalid text", name, i)
		}
		if len(argument.Value) > MaxArgBytes {
			return Declaration{}, fmt.Errorf("service %q command argument %d exceeds %d bytes", name, i, MaxArgBytes)
		}
		total += len(argument.Value)
		declaration.Argv[i] = argument.Value
	}
	if declaration.Argv[0] == "" {
		return Declaration{}, fmt.Errorf("service %q command argv[0] must not be empty", name)
	}
	if total > MaxArgvBytes {
		return Declaration{}, fmt.Errorf("service %q command exceeds %d aggregate bytes", name, MaxArgvBytes)
	}
	if port != nil {
		if port.Kind != yaml.ScalarNode || port.Tag != "!!int" || !canonicalPositiveInteger(port.Value) {
			return Declaration{}, fmt.Errorf("service %q port must be a canonical integer", name)
		}
		parsed, err := strconv.ParseInt(port.Value, 10, 32)
		if err != nil || parsed < 1 || parsed > 65535 || parsed == 50051 {
			return Declaration{}, fmt.Errorf("service %q port must be between 1 and 65535 other than 50051", name)
		}
		value := int32(parsed)
		declaration.Port = &value
	}
	return declaration, nil
}

func canonicalPositiveInteger(value string) bool {
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func inspectNode(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode || node.Anchor != "" {
		return errors.New("YAML aliases and anchors are not allowed")
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]struct{}, len(node.Content)/2)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if key.Kind != yaml.ScalarNode {
				return errors.New("YAML mapping keys must be scalars")
			}
			if key.Tag == "!!merge" || key.Value == "<<" {
				return errors.New("YAML merge keys are not allowed")
			}
			identity := key.Tag + "\x00" + key.Value
			if _, exists := seen[identity]; exists {
				return fmt.Errorf("duplicate YAML mapping key %q", key.Value)
			}
			seen[identity] = struct{}{}
		}
	}
	for _, child := range node.Content {
		if err := inspectNode(child); err != nil {
			return err
		}
	}
	return nil
}
