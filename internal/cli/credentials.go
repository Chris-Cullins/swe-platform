package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/spf13/cobra"
)

func newCredentialsCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "credentials", Short: "Manage agent credential profiles"}
	cmd.AddCommand(newCredentialsCreateCommand(), newCredentialsRotateCommand(), newCredentialsListCommand(), newCredentialsDeleteCommand())
	return cmd
}

func newCredentialsCreateCommand() *cobra.Command {
	var agent string
	var stdin bool
	cmd := &cobra.Command{Use: "create NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !stdin {
			return fmt.Errorf("--api-key-stdin is required")
		}
		key, err := readAPIKey(cmd.InOrStdin())
		if err != nil {
			return err
		}
		defer clear(key)
		clients, err := newKubeClients()
		if err != nil {
			return err
		}
		namespace, _ := cmd.Flags().GetString("namespace")
		if err := createCredential(cmd.Context(), clients.Client, namespace, args[0], agent, key); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "credential profile %s created (agent %s, type %s)\n", args[0], agent, platformv1alpha1.AgentCredentialTypeAPIKey)
		return nil
	}}
	cmd.Flags().StringVar(&agent, "agent", "", "Agent adapter allowed to use this credential")
	cmd.Flags().BoolVar(&stdin, "api-key-stdin", false, "Read the API key from standard input")
	_ = cmd.MarkFlagRequired("agent")
	return cmd
}

func newCredentialsRotateCommand() *cobra.Command {
	var stdin bool
	cmd := &cobra.Command{Use: "rotate NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !stdin {
			return fmt.Errorf("--api-key-stdin is required")
		}
		key, err := readAPIKey(cmd.InOrStdin())
		if err != nil {
			return err
		}
		defer clear(key)
		clients, err := newKubeClients()
		if err != nil {
			return err
		}
		namespace, _ := cmd.Flags().GetString("namespace")
		if err := rotateCredential(cmd.Context(), clients.Client, namespace, args[0], key); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "credential profile %s rotated\n", args[0])
		return nil
	}}
	cmd.Flags().BoolVar(&stdin, "api-key-stdin", false, "Read the API key from standard input")
	return cmd
}

func newCredentialsListCommand() *cobra.Command {
	return &cobra.Command{Use: "list", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		clients, err := newKubeClients()
		if err != nil {
			return err
		}
		namespace, _ := cmd.Flags().GetString("namespace")
		return listCredentials(cmd.Context(), clients.Client, namespace, cmd.OutOrStdout())
	}}
}

func newCredentialsDeleteCommand() *cobra.Command {
	return &cobra.Command{Use: "delete NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		clients, err := newKubeClients()
		if err != nil {
			return err
		}
		namespace, _ := cmd.Flags().GetString("namespace")
		if err := deleteCredential(cmd.Context(), clients.Client, namespace, args[0]); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "credential profile %s deleted\n", args[0])
		return nil
	}}
}

func readAPIKey(r io.Reader) ([]byte, error) {
	const readLimit = platformv1alpha1.AgentCredentialAPIKeyMaxBytes + 3 // Maximum value, CRLF, and one overflow byte.
	value, err := io.ReadAll(io.LimitReader(r, readLimit))
	if err != nil {
		return nil, fmt.Errorf("read API key: %w", err)
	}
	if len(value) == readLimit {
		return nil, fmt.Errorf("API key exceeds maximum size")
	}
	if bytes.HasSuffix(value, []byte("\r\n")) {
		value = value[:len(value)-2]
	} else if bytes.HasSuffix(value, []byte("\n")) {
		value = value[:len(value)-1]
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("API key must not be empty")
	}
	if len(value) > platformv1alpha1.AgentCredentialAPIKeyMaxBytes {
		return nil, fmt.Errorf("API key exceeds maximum size")
	}
	if !utf8.Valid(value) {
		return nil, fmt.Errorf("API key must be valid UTF-8")
	}
	if bytes.IndexByte(value, 0) >= 0 {
		return nil, fmt.Errorf("API key must not contain NUL")
	}
	return append([]byte(nil), value...), nil
}

func createCredential(ctx context.Context, c client.Client, namespace, name, agent string, key []byte) error {
	if len(validation.IsDNS1123Subdomain(name)) != 0 || agent == "" {
		return fmt.Errorf("credential profile name and agent are required and name must be a Kubernetes DNS subdomain")
	}
	desired := platformv1alpha1.AgentCredentialProfileSpec{Adapter: agent, CredentialType: platformv1alpha1.AgentCredentialTypeAPIKey}
	profile := &platformv1alpha1.AgentCredentialProfile{TypeMeta: metav1.TypeMeta{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "AgentCredentialProfile"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: desired}
	if err := c.Create(ctx, profile); err != nil {
		var existing platformv1alpha1.AgentCredentialProfile
		if getErr := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &existing); getErr != nil {
			return fmt.Errorf("create credential profile %q: %w", name, err)
		}
		if existing.Spec != desired {
			return fmt.Errorf("credential profile %q already exists with different intent", name)
		}
		profile = &existing
	}
	if profile.UID == "" {
		return fmt.Errorf("credential profile %q has no assigned UID", name)
	}
	secret := credentialSecret(profile, key)
	defer clear(secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey])
	if err := c.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return redactCredentialError("store credential data", err)
		}
		metadata, getErr := getCredentialSecretMetadata(ctx, c, profile)
		if getErr != nil {
			return redactCredentialError("check backing credential ownership", getErr)
		}
		if !exactCredentialSecretOwner(profile, metadata) {
			return fmt.Errorf("backing credential data for profile %q is not safely managed by that profile", profile.Name)
		}
		var existing corev1.Secret
		if getErr := c.Get(ctx, client.ObjectKeyFromObject(secret), &existing); getErr != nil {
			return redactCredentialError("check backing credential data", getErr)
		}
		defer clearCredentialData(existing.Data)
		if existing.UID != metadata.UID || existing.ResourceVersion != metadata.ResourceVersion {
			return fmt.Errorf("backing credential data for profile %q changed during validation", profile.Name)
		}
		return validateCredentialSecret(profile, &existing)
	}
	return nil
}

func credentialSecret(profile *platformv1alpha1.AgentCredentialProfile, key []byte) *corev1.Secret {
	controller, blockDeletion := true, true
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: platformv1alpha1.AgentCredentialSecretName(profile.UID), Namespace: profile.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "AgentCredentialProfile", Name: profile.Name, UID: profile.UID, Controller: &controller, BlockOwnerDeletion: &blockDeletion}}}, Type: platformv1alpha1.AgentCredentialAPIKeySecretType, Data: map[string][]byte{platformv1alpha1.AgentCredentialAPIKeySecretKey: append([]byte(nil), key...)}}
}

func validateCredentialSecret(profile *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) error {
	value, hasKey := secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey]
	if !exactCredentialSecretOwner(profile, secret) || secret.Type != platformv1alpha1.AgentCredentialAPIKeySecretType || len(secret.Data) != 1 || !hasKey || len(value) == 0 ||
		len(value) > platformv1alpha1.AgentCredentialAPIKeyMaxBytes || !utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("backing credential data for profile %q is not safely managed by that profile", profile.Name)
	}
	return nil
}

func getCredentialSecretMetadata(ctx context.Context, c client.Reader, profile *platformv1alpha1.AgentCredentialProfile) (*metav1.PartialObjectMetadata, error) {
	metadata := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Secret"}}
	err := c.Get(ctx, types.NamespacedName{Namespace: profile.Namespace, Name: platformv1alpha1.AgentCredentialSecretName(profile.UID)}, metadata)
	return metadata, err
}

func exactCredentialSecretOwner(profile *platformv1alpha1.AgentCredentialProfile, object metav1.Object) bool {
	owner := metav1.GetControllerOf(object)
	return len(object.GetOwnerReferences()) == 1 && owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() &&
		owner.Kind == "AgentCredentialProfile" && owner.Name == profile.Name && owner.UID == profile.UID
}

func clearCredentialData(data map[string][]byte) {
	for _, value := range data {
		clear(value)
	}
}

func rotateCredential(ctx context.Context, c client.Client, namespace, name string, key []byte) error {
	var profile platformv1alpha1.AgentCredentialProfile
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &profile); err != nil {
		return fmt.Errorf("get credential profile %q: %w", name, err)
	}
	if profile.Spec.CredentialType != platformv1alpha1.AgentCredentialTypeAPIKey {
		return fmt.Errorf("credential profile %q is not an API key profile", name)
	}
	metadata, err := getCredentialSecretMetadata(ctx, c, &profile)
	if err != nil {
		return redactCredentialError(fmt.Sprintf("get backing credential ownership for profile %q", name), err)
	}
	if !exactCredentialSecretOwner(&profile, metadata) {
		return fmt.Errorf("backing credential data for profile %q is not safely managed by that profile", profile.Name)
	}
	var secret corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: platformv1alpha1.AgentCredentialSecretName(profile.UID)}, &secret); err != nil {
		return redactCredentialError(fmt.Sprintf("get backing credential data for profile %q", name), err)
	}
	defer clearCredentialData(secret.Data)
	if secret.UID != metadata.UID || secret.ResourceVersion != metadata.ResourceVersion {
		return fmt.Errorf("backing credential data for profile %q changed during validation", profile.Name)
	}
	if err := validateCredentialSecret(&profile, &secret); err != nil {
		return err
	}
	secret.Data = map[string][]byte{platformv1alpha1.AgentCredentialAPIKeySecretKey: append([]byte(nil), key...)}
	defer clear(secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey])
	if err := c.Update(ctx, &secret); err != nil {
		return redactCredentialError(fmt.Sprintf("rotate credential profile %q", name), err)
	}
	return nil
}

func listCredentials(ctx context.Context, c client.Client, namespace string, out io.Writer) error {
	var profiles platformv1alpha1.AgentCredentialProfileList
	if err := c.List(ctx, &profiles, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list credential profiles: %w", err)
	}
	fmt.Fprintln(out, "NAME\tAGENT\tTYPE")
	for _, profile := range profiles.Items {
		fmt.Fprintf(out, "%s\t%s\t%s\n", profile.Name, profile.Spec.Adapter, profile.Spec.CredentialType)
	}
	return nil
}

func deleteCredential(ctx context.Context, c client.Client, namespace, name string) error {
	profile := &platformv1alpha1.AgentCredentialProfile{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if err := c.Delete(ctx, profile); err != nil {
		return fmt.Errorf("delete credential profile %q: %w", name, err)
	}
	return nil
}

type credentialError struct {
	message string
	cause   error
}

func (e credentialError) Error() string { return e.message }
func (e credentialError) Unwrap() error { return e.cause }
func redactCredentialError(action string, cause error) error {
	return credentialError{message: action + " failed", cause: cause}
}
