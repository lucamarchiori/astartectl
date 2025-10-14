package migrationutils

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// CopyIfExists safely copies a value from source map to destination map if the key exists
// and the value is not nil. This prevents adding nil/zero values to the destination when
// a field is not present in the source, which is useful when migrating between API versions
// where fields may be optional.
func CopyIfExists(source *unstructured.Unstructured, dest *unstructured.Unstructured, sPath []string, dPath []string) error {
	if source == nil || dest == nil {
		return fmt.Errorf("source or dest is nil")
	}

	sourceField, found, err := unstructured.NestedFieldNoCopy(source.Object, sPath...)
	if err != nil {
		slog.Error("error retrieving field", "field", sourceField, "err", err)
	}

	if found && sourceField != nil && sourceField != "" {
		if err := unstructured.SetNestedField(dest.Object, sourceField, dPath...); err != nil {
			return fmt.Errorf("error setting field %s: %v", dPath, err)
		}
	}

	return nil
}

// Given two component specs, merge their resources fields if present.
// Resources are of type v1.ResourceRequirements, which has Limits and Requests maps.
// If a component does not have resources defined, it is ignored.
// If both components have resources defined, their Limits and Requests are summed.
// If only one component has resources defined, that is returned as is.
// If neither component has resources defined, nil is returned.
// Returns a map[string]interface{} representing the merged resources, or nil.
func MergeResources(comp1 interface{}, comp2 interface{}) map[string]interface{} {
	// Helper to extract the resources map safely.
	extract := func(comp interface{}) map[string]interface{} {
		if comp == nil {
			return nil
		}
		m, ok := comp.(map[string]interface{})
		if !ok || m == nil {
			return nil
		}
		raw, ok := m["resources"]
		if !ok || raw == nil {
			return nil
		}
		rmap, ok := raw.(map[string]interface{})
		if !ok || rmap == nil {
			return nil
		}
		return rmap
	}

	r1 := extract(comp1)
	r2 := extract(comp2)

	if r1 == nil && r2 == nil {
		return nil
	}
	if r1 == nil {
		// Return a shallow copy so callers don't mutate original
		out := make(map[string]interface{}, len(r2))
		for k, v := range r2 {
			out[k] = v
		}
		return out
	}
	if r2 == nil {
		out := make(map[string]interface{}, len(r1))
		for k, v := range r1 {
			out[k] = v
		}
		return out
	}

	// Both present: start with a deep-ish copy of r1 then merge r2 in.
	merged := make(map[string]interface{}, len(r1))
	for k, v := range r1 {
		merged[k] = v
	}

	// Helper to merge one resource type (limits/requests)
	mergeQuantityMap := func(key string) {
		var m1 map[string]interface{}
		if existing, ok := merged[key]; ok && existing != nil {
			m1, _ = existing.(map[string]interface{})
		}
		var m2 map[string]interface{}
		if v, ok := r2[key]; ok && v != nil {
			m2, _ = v.(map[string]interface{})
		}
		if m1 == nil && m2 == nil {
			return
		}
		if m1 == nil { // copy m2
			cp := make(map[string]interface{}, len(m2))
			for k, v := range m2 {
				cp[k] = v
			}
			merged[key] = cp
			return
		}
		if m2 == nil { // keep m1 as is
			merged[key] = m1
			return
		}
		// Both non-nil: sum cpu and memory if present; copy other keys from m2 if absent.
		for k, v2 := range m2 {
			if v2 == nil {
				continue
			}
			if v1, ok := m1[k]; ok && v1 != nil && (k == "cpu" || k == "memory") {
				q1, err1 := resource.ParseQuantity(fmt.Sprint(v1))
				q2, err2 := resource.ParseQuantity(fmt.Sprint(v2))
				if err1 == nil && err2 == nil {
					q1.Add(q2)
					m1[k] = q1.String()
					continue
				}
			}
			// If key not present or not summable, copy/override with second value
			if _, ok := m1[k]; !ok {
				m1[k] = v2
			}
		}
		merged[key] = m1
	}

	mergeQuantityMap("limits")
	mergeQuantityMap("requests")

	return merged
}

// ParseCassandraStrNodes reads spec.cassandra.nodes and returns a []interface{} suitable
// for unstructured.SetNestedSlice under spec.cassandra.connection.nodes.
// It accepts legacy comma-separated strings or already-array formats.
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

		port := int64(defaultCassandraPort)
		if hasPort {
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
func MergeAdditionalEnv(apiEnv interface{}, backendEnv interface{}) []interface{} {
	const anonPrefix = "__anon_"

	// Extract slices
	toSlice := func(v interface{}) []interface{} {
		if v == nil {
			return nil
		}
		if s, ok := v.([]interface{}); ok {
			return s
		}
		return nil
	}

	apiSlice := toSlice(apiEnv)
	backSlice := toSlice(backendEnv)
	if len(apiSlice) == 0 && len(backSlice) == 0 {
		return nil
	}

	// Track variables by name to handle precedence
	byName := make(map[string]interface{})
	var order []string

	addVariable := func(item interface{}) {
		m, ok := item.(map[string]interface{})
		if !ok || m == nil {
			return
		}

		// Extract variable name
		var name string
		if n, ok := m["name"].(string); ok {
			name = n
		} else if n, ok := m["Name"].(string); ok {
			name = n
		}

		if name == "" {
			// Handle anonymous variables with synthetic keys
			name = fmt.Sprintf("%s%d", anonPrefix, len(order))
		}

		if _, exists := byName[name]; !exists {
			order = append(order, name)
		}
		byName[name] = m
	}

	// Add API variables first
	for _, item := range apiSlice {
		addVariable(item)
	}
	// Backend variables override API variables with same name
	for _, item := range backSlice {
		addVariable(item)
	}

	// Build output preserving order
	result := make([]interface{}, 0, len(order))
	for _, name := range order {
		if v, ok := byName[name]; ok {
			result = append(result, v)
		}
	}
	return result
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

func DumpYamlToUnstructured(y []byte) (*unstructured.Unstructured, error) {
	var obj map[string]interface{}
	if err := yaml.Unmarshal(y, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

// unstructuredToJSON converts an unstructured object to JSON bytes.
func unstructuredToJSON(in *unstructured.Unstructured) ([]byte, error) {
	out, err := json.Marshal(in.Object)
	if err != nil {
		return []byte{}, err
	}
	return out, nil
}

// unstructuredToYAML converts an unstructured object to YAML bytes.
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
