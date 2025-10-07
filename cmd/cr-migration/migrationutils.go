package migrationutils

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// CopyIfExists copies a value from source object to destination object if the source key exists
// and the value is not nil. This prevents adding nil/zero values to the destination when
// a field is not present in the source or is explicitly set to nil/empty (e.g. by defaulting annotations).
func CopyIfExists(source *unstructured.Unstructured, dest *unstructured.Unstructured, sPath []string, dPath []string) error {
	if source == nil || dest == nil {
		return fmt.Errorf("source or dest is nil")
	}

	// Retrieve the field from source only if it exists
	sourceField, found, err := unstructured.NestedFieldNoCopy(source.Object, sPath...)
	if err != nil {
		slog.Error("error retrieving field", "field", sourceField, "err", err)
	}

	// If found and not nil/empty, set it in dest
	if found && sourceField != nil && sourceField != "" {
		if err := unstructured.SetNestedField(dest.Object, sourceField, dPath...); err != nil {
			return fmt.Errorf("error setting field %s: %v", dPath, err)
		}
	}

	return nil
}

// SumResourceRequirements sums two v1.ResourceRequirements objects.
func SumResourceRequirements(a, b v1.ResourceRequirements) v1.ResourceRequirements {
	result := v1.ResourceRequirements{
		Limits:   sumResourceLists(a.Limits, b.Limits),
		Requests: sumResourceLists(a.Requests, b.Requests),
	}
	return result
}

// sumResourceLists sums two v1.ResourceList objects.
func sumResourceLists(rl1, rl2 v1.ResourceList) v1.ResourceList {
	if rl1 == nil && rl2 == nil {
		return nil
	}

	result := v1.ResourceList{}

	// Copy all from rl1
	if rl1 != nil {
		for k, v := range rl1 {
			result[k] = v.DeepCopy()
		}
	}

	// Add rl2 onto result
	if rl2 != nil {
		for k, v := range rl2 {
			if existing, ok := result[k]; ok {
				sum := existing.DeepCopy()
				sum.Add(v)
				result[k] = sum
			} else {
				result[k] = v.DeepCopy()
			}
		}
	}

	return result
}

// UnstructuredToResourceRequirements converts a generic interface (typically map[string]interface{})
// representing Kubernetes ResourceRequirements into a concrete v1.ResourceRequirements.
func UnstructuredToResourceRequirements(in interface{}) (v1.ResourceRequirements, error) {
	// If already the right type, return as-is
	if rr, ok := in.(v1.ResourceRequirements); ok {
		return rr, nil
	}

	m, ok := in.(map[string]interface{})
	if !ok || m == nil {
		return v1.ResourceRequirements{}, fmt.Errorf("unsupported resources type: %T", in)
	}

	rr := v1.ResourceRequirements{}

	// Parse limits
	if limRaw, ok := m["limits"].(map[string]interface{}); ok {
		rr.Limits = parseResourceListFromMap(limRaw)
	}

	// Parse requests
	if reqRaw, ok := m["requests"].(map[string]interface{}); ok {
		rr.Requests = parseResourceListFromMap(reqRaw)
	}

	return rr, nil
}

// UnstructuredToEnvVarList converts a generic interface (typically []interface{})
// representing a list of environment variables into a concrete []v1.EnvVar.
func UnstructuredToEnvVarList(in interface{}) ([]v1.EnvVar, error) {
	// If already the right type, return as-is
	if envVars, ok := in.([]v1.EnvVar); ok {
		return envVars, nil
	}

	slice, ok := in.([]interface{})
	if !ok || slice == nil {
		return []v1.EnvVar{}, fmt.Errorf("unsupported env var list type: %T", in)
	}

	var envVars []v1.EnvVar
	for _, item := range slice {
		m, ok := item.(map[string]interface{})
		if !ok || m == nil {
			slog.Warn("skipping unsupported env var item type", "type", item)
			continue
		}

		var ev v1.EnvVar
		if name, ok := m["name"].(string); ok {
			ev.Name = name
		}
		if value, ok := m["value"].(string); ok {
			ev.Value = value
		}

		// Note: skipping valueFrom for simplicity; can be added if needed

		envVars = append(envVars, ev)
	}

	return envVars, nil
}

// EnvVarListToUnstructured converts a slice of v1.EnvVar to a []interface{}
// suitable to be embedded in an unstructured object. Only name and value are
// preserved, consistent with UnstructuredToEnvVarList.
func EnvVarListToUnstructured(in []v1.EnvVar) []interface{} {
	if in == nil {
		return nil
	}
	out := make([]interface{}, 0, len(in))
	for _, ev := range in {
		m := map[string]interface{}{}
		if ev.Name != "" {
			m["name"] = ev.Name
		}
		if ev.Value != "" {
			m["value"] = ev.Value
		}
		// Note: valueFrom is intentionally omitted to mirror the
		// UnstructuredToEnvVarList behavior.
		out = append(out, m)
	}
	return out
}

// ResourceRequirementsToUnstructured converts v1.ResourceRequirements to a map[string]interface{}
// suitable to be embedded in an unstructured object.
func ResourceRequirementsToUnstructured(rr v1.ResourceRequirements) map[string]interface{} {
	out := map[string]interface{}{}
	if rr.Limits != nil {
		out["limits"] = resourceListToMap(rr.Limits)
	}
	if rr.Requests != nil {
		out["requests"] = resourceListToMap(rr.Requests)
	}
	return out
}

// parseResourceListFromMap parses a map[string]interface{} of resource quantities
// (e.g. {"cpu":"500m","memory":"128Mi"}) into v1.ResourceList.
func parseResourceListFromMap(in map[string]interface{}) v1.ResourceList {
	rl := v1.ResourceList{}
	for k, v := range in {
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		q, err := resource.ParseQuantity(s)
		if err != nil {
			slog.Warn("failed parsing quantity", "key", k, "value", s, "err", err)
			continue
		}
		switch strings.ToLower(k) {
		case "cpu":
			rl[v1.ResourceCPU] = q
		case "memory":
			rl[v1.ResourceMemory] = q
		default:
			// Allow custom keys; store as-is
			rl[v1.ResourceName(k)] = q
		}
	}
	return rl
}

// resourceListToMap converts v1.ResourceList to map[string]interface{} with string quantities.
func resourceListToMap(rl v1.ResourceList) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range rl {
		// Use canonical string representation
		out[string(k)] = v.String()
	}
	return out
}

// ParseCassandraStrNodes reads spec.cassandra.nodes and returns a []interface{} suitable
// for unstructured.SetNestedSlice under spec.cassandra.connection.nodes.
// It accepts legacy comma-separated strings and returns a slice of ip/port maps.
func ParseCassandraStrNodes(oldSpec *unstructured.Unstructured) []interface{} {
	const defaultCassandraPort = 9042

	oldNodes, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "cassandra", "nodes")
	if err != nil {
		slog.Error("error retrieving cassandra nodes", "err", err)
		return []interface{}{}
	}
	if !found || oldNodes == nil {
		slog.Error("spec.cassandra.nodes field is missing or empty in the input CR. Resulting CR will have no cassandra connection nodes.")
		return []interface{}{}
	}

	// Legacy: comma-separated string "host:port,host2:port"
	oldNodesStr, ok := oldNodes.(string)
	if !ok {
		slog.Warn("spec.cassandra.nodes has unsupported type; skipping nodes conversion")
		return []interface{}{}
	}

	// Parse the comma-separated string
	// Nodes are of the form host:port, multiple nodes separated by commas
	// e.g. "cassandra1.example.com:9042,cassandra2.example.com:9042"
	var nodes []interface{}
	for _, entry := range strings.Split(oldNodesStr, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		host, portStr, hasPort := strings.Cut(entry, ":")
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}

		// Default port if not specified
		port := int64(defaultCassandraPort)
		if hasPort {
			// Set port if valid
			if p, err := strconv.ParseInt(strings.TrimSpace(portStr), 10, 64); err == nil {
				port = p
			}
		}

		nodes = append(nodes, map[string]interface{}{
			"host": host,
			"port": port,
		})
	}

	return nodes
}

// MergeAdditionalEnv merges two additionalEnv fields (from API and Backend specs)
// Backend variables take precedence in case of conflicts (same name)
func MergeAdditionalEnv(apiEnv []v1.EnvVar, backendEnv []v1.EnvVar) []v1.EnvVar {
	// Delete duplicates from apiEnv
	for _, be := range backendEnv {
		for i, ae := range apiEnv {
			if ae.Name == be.Name {
				// Remove from apiEnv
				apiEnv = append(apiEnv[:i], apiEnv[i+1:]...)
				break
			}
		}
	}

	// Merge
	return append(apiEnv, backendEnv...)
}

// dumpResourceToYAMLFile dumps an unstructured resource to a YAML file at the given filepath.
func DumpResourceToYAMLFile(in *unstructured.Unstructured, filepath string) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	y, err := unstructuredToYAML(in)
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath, []byte(y), 0644); err != nil {
		return err
	}

	return nil
}

// DumpYamlToUnstructured converts YAML to an unstructured object.
func DumpYamlToUnstructured(y []byte) (*unstructured.Unstructured, error) {
	var obj map[string]interface{}
	if err := yaml.Unmarshal(y, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

// unstructuredToJSON converts an unstructured object to JSON.
func unstructuredToJSON(in *unstructured.Unstructured) ([]byte, error) {
	out, err := json.Marshal(in.Object)
	if err != nil {
		return []byte{}, err
	}
	return out, nil
}

// unstructuredToYAML converts an unstructured object to YAML.
func unstructuredToYAML(in *unstructured.Unstructured) ([]byte, error) {
	j, err := unstructuredToJSON(in)
	if err != nil {
		return []byte{}, err
	}
	out, err := yaml.JSONToYAML(j)
	if err != nil {
		return []byte{}, err
	}
	return out, nil
}
