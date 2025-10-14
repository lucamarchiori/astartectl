package v1alpha2tov2alpha1

import (
	"fmt"
	"log/slog"

	migrationutils "github.com/astarte-platform/astartectl/cmd/cr-migration"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Type aliases for better readability
type v1alpha2 = unstructured.Unstructured
type v2alpha1 = unstructured.Unstructured

// convertCassandraConnectionSpec converts the spec.cassandra.connection section from v1alpha2 to v2alpha1
func convertCassandraConnectionSpec(oldSpec *unstructured.Unstructured) (newConnection *unstructured.Unstructured) {
	newConnection = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldConnection, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "cassandra", "connection")
	if err != nil {
		slog.Error("error retrieving cassandra connection spec", "err", err)
	}

	if !found || oldConnection == nil {
		slog.Error("spec.cassandra.connection section is missing or empty in the input CR. Resulting CR will have no cassandra connection spec resulting in a invalid Astarte CR.")
	}

	// The following fields are deep copied from the old connection to the new connection. No changes here
	dc := []string{"poolSize", "sslConfiguration"}
	for _, f := range dc {
		sourcePath := []string{"spec", "cassandra", "connection", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newConnection, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	// spec.cassandra.connection.secret -> spec.cassandra.connection.credentialsSecret
	slog.Warn("spec.cassandra.connection.username is no longer supported in v2alpha1 and will be ignored. You will need to set it in the credentialsSecret.")
	slog.Warn("spec.cassandra.connection.password is no longer supported in v2alpha1 and will be ignored. You will need to set it in the credentialsSecret.")
	slog.Warn("spec.cassandra.connection.autodiscovery is no longer supported in v2alpha1 and will be ignored.")

	sourcePath := []string{"spec", "cassandra", "connection", "secret"}
	destPath := []string{"credentialsSecret"}
	err = migrationutils.CopyIfExists(oldSpec, newConnection, sourcePath, destPath)
	if err != nil {
		slog.Error("error copying secret to credentialsSecret", "err", err)
	}

	// spec.cassandra.connection.nodes conversion
	if nodes := migrationutils.ParseCassandraStrNodes(oldSpec); len(nodes) > 0 {
		if err := unstructured.SetNestedSlice(newConnection.Object, nodes, "nodes"); err != nil {
			slog.Error("error setting cassandra connection nodes", "err", err)
		}
	}

	return newConnection
}

// convertCassandraSpec converts the spec.cassandra section from v1alpha2 to v2alpha1
func convertCassandraSpec(oldSpec *unstructured.Unstructured) (newCassandra *unstructured.Unstructured) {
	newCassandra = &unstructured.Unstructured{Object: map[string]interface{}{}}

	oldCassandra, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "cassandra")
	if err != nil {
		slog.Error("error retrieving cassandra spec", "err", err)
		return newCassandra
	}

	if !found || oldCassandra == nil {
		slog.Error("spec.cassandra section is missing or empty in the input CR. Resulting CR will have no cassandra spec resulting in a invalid Astarte CR.")
		return newCassandra
	}

	slog.Info("The following fields are no longer supported and will be ignored if present in the source CR: cassandra.deploy, cassandra.replicas, cassandra.image, cassandra.version, cassandra.storage, cassandra.maxHeapSize, cassandra.heapNewSize, cassandra.resources.")

	// spec.astarteSystemKeyspace is now spec.cassandra.astarteSystemKeyspace
	// Build the inner cassandra object content directly (do not nest a top-level "cassandra" key here)
	err = migrationutils.CopyIfExists(oldSpec, newCassandra, []string{"spec", "astarteSystemKeyspace"}, []string{"astarteSystemKeyspace"})
	if err != nil {
		slog.Error("error copying astarteSystemKeyspace", "err", err)
	}

	// If spec.cassandra.deploy is true, ask the user for cassandra.connection.credentialsSecret details
	deploy, foundDepoly, errDeploy := unstructured.NestedBool(oldSpec.Object, "spec", "cassandra", "deploy")
	if deploy && foundDepoly && errDeploy == nil {
		slog.Error("spec.cassandra.deploy is set to true. With the new CR, you will need to deploy Scylla yourself and configure cassandra.connection accordingly. Not doing so will result in a broken Astarte deployment.")

		cassandraConnectionSecretName := ""
		cassandraConnectionSecretUsernameKey := ""
		cassandraConnectionSecretPasswordKey := ""

		// Since the deploy was ture, we ask user for pointers to the new connection
		fmt.Print("new cassandra.connection.credentialsSecret.name: ")
		fmt.Scanln(&cassandraConnectionSecretName)
		fmt.Print("new cassandra.connection.credentialsSecret.usernameKey: ")
		fmt.Scanln(&cassandraConnectionSecretUsernameKey)
		fmt.Print("new cassandra.connection.credentialsSecret.passwordKey: ")
		fmt.Scanln(&cassandraConnectionSecretPasswordKey)

		slog.Info("Cassandra connection credentialsSecret set to", "name", cassandraConnectionSecretName, "usernameKey", cassandraConnectionSecretUsernameKey, "passwordKey", cassandraConnectionSecretPasswordKey)
		// Set the values in the new cassandra connection
		newCassandra.Object["credentialsSecret"] = map[string]interface{}{
			"name":        cassandraConnectionSecretName,
			"usernameKey": cassandraConnectionSecretUsernameKey,
			"passwordKey": cassandraConnectionSecretPasswordKey,
		}

		return newCassandra
	}

	// spec.cassandra.connection conversion
	if conn := convertCassandraConnectionSpec(oldSpec); conn != nil && len(conn.Object) > 0 {
		unstructured.SetNestedField(newCassandra.Object, conn.Object, "connection")
	}

	// Always return the assembled Cassandra subresource (may be empty if missing in source)
	return newCassandra
}

func convertRabbitMQConnectionSpec(oldSpec *unstructured.Unstructured) (newConnection *unstructured.Unstructured) {
	slog.Info("Converting RabbitMQ connection spec")
	newConnection = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldConnection, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "rabbitmq", "connection")
	if err != nil {
		slog.Error("error retrieving rabbitmq connection spec", "err", err)
	}

	if !found || oldConnection == nil {
		slog.Error("spec.rabbitmq.connection section is missing or empty in the input CR. Resulting CR will have no rabbitmq connection spec resulting in a invalid Astarte CR.")
	}

	// The following fields are deep copied from the old connection to the new connection. No changes here
	dc := []string{"host", "port", "sslConfiguration", "virtualHost"}
	for _, f := range dc {
		sourcePath := []string{"spec", "rabbitmq", "connection", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newConnection, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Warn("spec.rabbitmq.connection.username is no longer supported in v2alpha1 and will be ignored. You will need to set it in the credentialsSecret.")
	slog.Warn("spec.rabbitmq.connection.password is no longer supported in v2alpha1 and will be ignored. You will need to set it in the credentialsSecret.")

	// spec.rabbitmq.connection.secret -> spec.rabbitmq.connection.credentialsSecret
	sourcePath := []string{"spec", "rabbitmq", "connection", "secret"}
	destPath := []string{"credentialsSecret"}
	err = migrationutils.CopyIfExists(oldSpec, newConnection, sourcePath, destPath)
	if err != nil {
		slog.Error("error copying secret to credentialsSecret", "err", err)
	}

	// Ask for port if not set
	if port, found, _ := unstructured.NestedInt64(newConnection.Object, "port"); !found || port == 0 {
		slog.Warn("rabbitmq.connection.port not set, please provide it:")
		fmt.Scanln(&port)
		newConnection.Object["port"] = int64(port)
		slog.Info(fmt.Sprintf("rabbitmq.connection.port set to %d", port))
	}

	// Ask for host if not set
	if host, found, _ := unstructured.NestedString(newConnection.Object, "host"); !found || host == "" {
		slog.Warn("rabbitmq.connection.host not set, please provide it:")
		fmt.Scanln(&host)
		newConnection.Object["host"] = host
		slog.Info(fmt.Sprintf("rabbitmq.connection.host set to %s", host))
	}

	slog.Info("RabbitMQ connection spec conversion completed")
	return newConnection
}

// convertRabbitMQSpec converts the spec.rabbitmq section from v1alpha2 to v2alpha1
func convertRabbitMQSpec(oldSpec *unstructured.Unstructured) (newRabbitMQ *unstructured.Unstructured) {
	slog.Info("Converting RabbitMQ spec")
	newRabbitMQ = &unstructured.Unstructured{Object: map[string]interface{}{}}

	oldRabbitMQ, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "rabbitmq")

	if err != nil {
		slog.Error("error retrieving rabbitmq spec", "err", err)
	}

	if !found || oldRabbitMQ == nil {
		slog.Error("spec.rabbitmq section is missing or empty in the input CR. Resulting CR will have no rabbitmq spec resulting in a invalid Astarte CR.")
		return newRabbitMQ
	}

	// The following fields are deep copied from the old rabbitmq to the new rabbitmq. No changes here
	dc := []string{"dataQueuesPrefix", "eventsExchangeName"}
	for _, f := range dc {
		sourcePath := []string{"spec", "rabbitmq", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newRabbitMQ, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Info("The following fields are no longer supported and will be ignored if present in the source CR: rabbitmq.deploy, rabbitmq.replicas, rabbitmq.image, rabbitmq.version, rabbitmq.storage, rabbitmq.resources, rabbitmq.additionalPlugins, rabbitmq.antiAffinity, rabbitmq.customAffinity.")

	// spec.rabbitmq.deploy is true, ask user for rabbitmq.connection.credentialsSecret details
	deploy, foundDepoy, errDepoy := unstructured.NestedBool(oldSpec.Object, "spec", "rabbitmq", "deploy")
	if deploy && foundDepoy && errDepoy == nil {
		slog.Error("spec.rabbitmq.deploy is set to true. With the new CR, you will need to deploy RabbitMQ yourself and configure rabbitmq.connection accordingly. Not doing so will result in a broken Astarte deployment.")

		rabbitmqConnectionSecretName := ""
		rabbitmqConnectionSecretUsernameKey := ""
		rabbitmqConnectionSecretPasswordKey := ""

		// Since the deploy was ture, we ask user for pointers to the new connection
		fmt.Print("new rabbitmq.connection.credentialsSecret.name: ")
		fmt.Scanln(&rabbitmqConnectionSecretName)
		fmt.Print("new rabbitmq.connection.credentialsSecret.usernameKey: ")
		fmt.Scanln(&rabbitmqConnectionSecretUsernameKey)
		fmt.Print("new rabbitmq.connection.credentialsSecret.passwordKey: ")
		fmt.Scanln(&rabbitmqConnectionSecretPasswordKey)

		slog.Info("RabbitMQ connection credentialsSecret set to", "name", rabbitmqConnectionSecretName, "usernameKey", rabbitmqConnectionSecretUsernameKey, "passwordKey", rabbitmqConnectionSecretPasswordKey)
		// Set the values in the new rabbitmq connection
		newRabbitMQ.Object["connection"] = map[string]interface{}{
			"credentialsSecret": map[string]interface{}{
				"name":        rabbitmqConnectionSecretName,
				"usernameKey": rabbitmqConnectionSecretUsernameKey,
				"passwordKey": rabbitmqConnectionSecretPasswordKey,
			},
		}

		return newRabbitMQ
	}

	// spec.rabbitmq.connection conversion
	if conn := convertRabbitMQConnectionSpec(oldSpec); conn != nil && len(conn.Object) > 0 {
		unstructured.SetNestedField(newRabbitMQ.Object, conn.Object, "connection")
	}

	slog.Info("RabbitMQ spec conversion completed")
	return newRabbitMQ
}

// convertVernemqSpec converts the spec.vernemq section from v1alpha2 to v2alpha1
func convertVernemqSpec(oldSpec *unstructured.Unstructured) (newVernemq *unstructured.Unstructured) {
	slog.Info("Converting VerneMQ spec")
	newVernemq = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldVernemq, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "vernemq")
	if err != nil {
		slog.Error("error retrieving vernemq spec", "err", err)
	}
	if !found || oldVernemq == nil {
		slog.Error("spec.vernemq section is missing or empty in the input CR. Resulting CR will have no vernemq spec resulting in a invalid Astarte CR.")
		return newVernemq
	}

	// The following fields are deep copied from the old vernemq to the new vernemq. No changes here
	dc := getAstarteGenericClusteredResourceFields()
	// Add vernemq-specific fields
	dc2 := []string{
		"host",
		"port",
		"storage",
		"caSecret",
		"deviceHeartbeatSeconds",
		"maxOfflineMessages",
		"persistentClientExpiration",
		"mirrorQueue",
		"sslListener",
		"sslListenerCertSecretName",
	}

	dc = append(dc, dc2...)

	for _, f := range dc {
		sourcePath := []string{"spec", "vernemq", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newVernemq, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	// Default port if not set
	if port, found, _ := unstructured.NestedInt64(newVernemq.Object, "port"); !found || port == 0 {
		newVernemq.Object["port"] = int64(1883)
		slog.Warn("vernemq.port not set, defaulting to 1883. Ensure this is correct.")
	}

	slog.Info("VerneMQ spec conversion completed")
	return newVernemq
}

func convertCsrRootCaSpec(oldSpec *unstructured.Unstructured) (newCsrRootCa *unstructured.Unstructured) {
	slog.Info("Converting CFSSL csrRootCa spec")
	newCsrRootCa = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldCsrRootCa, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "cfssl", "csrRootCa")
	if err != nil {
		slog.Error("error retrieving cfssl csrRootCa spec", "err", err)
	}
	if !found || oldCsrRootCa == nil {
		slog.Warn("spec.cfssl.csrRootCa section is missing or empty in the input CR. Resulting CR will have no cfssl csrRootCa spec.")
		return newCsrRootCa
	}

	// The following fields are deep copied from the old csrRootCa to the new csrRootCa. No changes here
	dc := []string{"CN", "key", "names"}
	for _, f := range dc {
		sourcePath := []string{"spec", "cfssl", "csrRootCa", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newCsrRootCa, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	// spec.cfssl.csrRootCa.ca.expiry -> spec.cfssl.csrRootCa.expiry
	sourcePath := []string{"spec", "cfssl", "csrRootCa", "ca", "expiry"}
	destPath := []string{"expiry"}
	err = migrationutils.CopyIfExists(oldSpec, newCsrRootCa, sourcePath, destPath)
	if err != nil {
		slog.Error("error copying ca.expiry to expiry", "err", err)
	}

	slog.Info("Converting CFSSL csrRootCa spec")
	return newCsrRootCa
}

func convertCaRootConfig(oldSpec *unstructured.Unstructured) (newCaRootConfig *unstructured.Unstructured) {
	slog.Info("Converting CFSSL caRootConfig spec")
	newCaRootConfig = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldCaRootConfig, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "cfssl", "caRootConfig")
	if err != nil {
		slog.Error("error retrieving cfssl caRootConfig spec", "err", err)
	}
	if !found || oldCaRootConfig == nil {
		slog.Warn("spec.cfssl.caRootConfig section is missing or empty in the input CR. Resulting CR will have no cfssl caRootConfig spec.")
		return newCaRootConfig
	}
	// The object previously at `signing.default` is now directly at `signingDefault`
	sourcePath := []string{"spec", "cfssl", "caRootConfig", "signing", "default"}
	destPath := []string{"signingDefault"}
	err = migrationutils.CopyIfExists(oldSpec, newCaRootConfig, sourcePath, destPath)
	if err != nil {
		slog.Error("error copying signing.default to signingDefault", "err", err)
	}
	slog.Info("caRootConfig conversion completed")
	return newCaRootConfig
}

// convertCfsslSpec converts the spec.cfssl section from v1alpha2 to v2alpha1
func convertCfsslSpec(oldSpec *unstructured.Unstructured) (newCfssl *unstructured.Unstructured) {
	slog.Info("Converting CFSSL spec")
	newCfssl = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldCfssl, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "cfssl")
	if err != nil {
		slog.Error("error retrieving cfssl spec", "err", err)
	}
	if !found || oldCfssl == nil {
		slog.Warn("spec.cfssl section is missing or empty in the input CR. Resulting CR will have no cfssl spec.")
		return newCfssl
	}

	// The following fields are deep copied from the old cfssl to the new cfssl. No changes here

	dc := []string{
		"deploy",
		"url",
		"caExpiry",
		"caSecret",
		"certificateExpiry",
		"dbConfig",
		"resources",
		"version",
		"image",
		"storage",
		"podLabels",
		"priorityClass",
	}

	for _, f := range dc {
		sourcePath := []string{"spec", "cfssl", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newCfssl, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	// spec.cfssl.csrRootCa conversion
	if csr := convertCsrRootCaSpec(oldSpec); csr != nil && len(csr.Object) > 0 {
		unstructured.SetNestedField(newCfssl.Object, csr.Object, "csrRootCa")
	}

	// spec.cfssl.caRootConfig conversion
	if caRoot := convertCaRootConfig(oldSpec); caRoot != nil && len(caRoot.Object) > 0 {
		unstructured.SetNestedField(newCfssl.Object, caRoot.Object, "caRootConfig")
	}

	slog.Info("CFSSL spec conversion completed")
	return newCfssl
}

// convertComponentsSpec converts the spec.components section from v1alpha2 to v2alpha1
func convertComponentsSpec(oldSpec *unstructured.Unstructured) (newComponents *unstructured.Unstructured) {
	slog.Info("Converting Components spec")
	newComponents = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldComponents, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components")
	if err != nil {
		slog.Error("error retrieving components spec", "err", err)
	}
	if !found || oldComponents == nil {
		slog.Warn("spec.components section is missing or empty in the input CR. Resulting CR will have no components spec.")
		return newComponents
	}

	if dup := convertDataUpdaterPlantSpec(oldSpec); dup != nil && len(dup.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, dup.Object, "dataUpdaterPlant")
	}

	if te := convertTriggerEngineSpec(oldSpec); te != nil && len(te.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, te.Object, "triggerEngine")
	}

	if ae := convertAppengineApiSpec(oldSpec); ae != nil && len(ae.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, ae.Object, "appengineApi")
	}

	if dash := convertDashboardSpec(oldSpec); dash != nil && len(dash.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, dash.Object, "dashboard")
	}

	if flow := convertFlowSpec(oldSpec); flow != nil && len(flow.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, flow.Object, "flow")
	}

	// Pairing API and Backend MERGED
	// Housekeeping API and Backend MERGED
	// Realm Management API and Backend MERGED
	// For these services, we need to merge the API and Backend specs into a single spec for each service
	// resources (memory/cpu) limits and requests are summed together

	if pairing := convertPairingSpec(oldSpec); pairing != nil && len(pairing.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, pairing.Object, "pairing")
	}

	if housekeeping := convertAstarteGenericComponentSpec(oldSpec, "housekeeping"); housekeeping != nil && len(housekeeping.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, housekeeping.Object, "housekeeping")
	}

	if realm := convertAstarteGenericComponentSpec(oldSpec, "realmManagement"); realm != nil && len(realm.Object) > 0 {
		unstructured.SetNestedField(newComponents.Object, realm.Object, "realmManagement")
	}

	slog.Info("Components spec conversion completed")
	return newComponents
}

// convertPairingSpec converts the spec.components.pairingApi and spec.components.pairingBackend sections from v1alpha2 to v2alpha1
func convertPairingSpec(oldSpec *unstructured.Unstructured) (newPairing *unstructured.Unstructured) {
	slog.Info("Converting Pairing spec: merging API and Backend specs")
	newPairing = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldPairing, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", "pairing")
	if err != nil {
		slog.Error("error retrieving pairing spec", "err", err)
	}
	if !found || oldPairing == nil {
		slog.Warn("spec.components.pairing section is missing or empty in the input CR. Resulting CR will have no pairing spec.")
		return newPairing
	}

	// Use convertAstarteGenericComponentSpec
	if newPairing = convertAstarteGenericComponentSpec(oldSpec, "pairing"); newPairing == nil {
		slog.Error("error converting pairing spec")
		return nil
	}

	slog.Info("Pairing spec conversion completed")
	return newPairing
}

// convertDataUpdaterPlantSpec converts the spec.components.dataUpdaterPlant section from v1alpha2 to v2alpha1
// DUP is basically unchanged between v1alpha2 and v2alpha1
func convertDataUpdaterPlantSpec(oldSpec *unstructured.Unstructured) (newDataUpdaterPlant *unstructured.Unstructured) {
	slog.Info("Converting DataUpdaterPlant spec")
	newDataUpdaterPlant = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldDataUpdaterPlant, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", "dataUpdaterPlant")
	if err != nil {
		slog.Error("error retrieving dataUpdaterPlant spec", "err", err)
	}
	if !found || oldDataUpdaterPlant == nil {
		slog.Warn("spec.components.dataUpdaterPlant section is missing or empty in the input CR. Resulting CR will have no dataUpdaterPlant spec.")
		return newDataUpdaterPlant
	}

	// The following fields are deep copied from the old dataUpdaterPlant to the new dataUpdaterPlant. No changes here
	dc1 := []string{
		"prefetchCount",
		"dataQueueCount",
	}

	dc2 := getAstarteGenericClusteredResourceFields()

	dc := append(dc1, dc2...)
	for _, f := range dc {
		sourcePath := []string{"spec", "components", "dataUpdaterPlant", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newDataUpdaterPlant, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Info("DataUpdaterPlant spec conversion completed")
	return newDataUpdaterPlant
}

// convertTriggerEngineSpec converts the spec.components.triggerEngine section from v1alpha2 to v2alpha1
// Merges API and Backend specs
func convertAstarteGenericComponentSpec(oldSpec *unstructured.Unstructured, componentName string) (newComponent *unstructured.Unstructured) {
	slog.Info("Converting " + componentName + " spec")
	newComponent = &unstructured.Unstructured{Object: map[string]interface{}{}}
	if componentName != "housekeeping" && componentName != "realmManagement" && componentName != "pairing" {
		slog.Error("convertAstarteGenericComponentSpec called with unsupported component name: " + componentName)
		return nil
	}

	// Each generic component has two sub-sections in v1alpha2: API and Backend (api and backend)
	// API contains fields of AstarteGenericAPISpec (see getAstarteGenericAPISpecFields)
	// Backend contains fields of AstarteGenericClusteredResource (see getAstarteGenericClusteredResourceFields)
	// For most fields, check if they exist in either API or Backend and copy them to the top-level of the new component spec,
	// if they exist in both, the Backend value takes precedence.
	// RESOURCES (memory/cpu) limits and requests are summed together if present in both API and Backend
	// Environment variables are merged, with Backend variables taking precedence in case of conflicts

	// Fetch api and backend subsections from old spec
	oldAPI, apiFound, errAPI := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", componentName, "api")
	if errAPI != nil {
		slog.Error("error retrieving "+componentName+" api spec", "err", errAPI)
	}
	oldBackend, backendFound, errBackend := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", componentName, "backend")
	if errBackend != nil {
		slog.Error("error retrieving "+componentName+" backend spec", "err", errBackend)
	}

	var apiMap map[string]interface{}
	if apiFound && oldAPI != nil {
		if m, ok := oldAPI.(map[string]interface{}); ok {
			apiMap = m
		}
	}
	var backendMap map[string]interface{}
	if backendFound && oldBackend != nil {
		if m, ok := oldBackend.(map[string]interface{}); ok {
			backendMap = m
		}
	}

	if apiMap == nil && backendMap == nil {
		slog.Warn("spec.components." + componentName + " section has neither api nor backend in the source CR")
		return newComponent
	}

	// Step 1: what are the fields we need to consider?
	// Build the union of candidate fields from API and Backend specs.
	// These are the only fields we will consider copying.
	unionFields := map[string]struct{}{}
	for _, f := range getAstarteGenericAPISpecFields() {
		unionFields[f] = struct{}{}
	}
	for _, f := range getAstarteGenericClusteredResourceFields() {
		unionFields[f] = struct{}{}
	}

	// First handle resources specially by summing CPU/memory
	if mergedRes := migrationutils.MergeResources(backendMap, apiMap); mergedRes != nil {
		newComponent.Object["resources"] = mergedRes
	}

	// Handle additionalEnv specially by merging arrays with backend precedence
	var apiEnv, backendEnv interface{}
	if apiMap != nil {
		apiEnv = apiMap["additionalEnv"]
	}
	if backendMap != nil {
		backendEnv = backendMap["additionalEnv"]
	}
	if mergedEnv := migrationutils.MergeAdditionalEnv(apiEnv, backendEnv); mergedEnv != nil {
		newComponent.Object["additionalEnv"] = mergedEnv
	}

	// For the remaining fields, copy with backend precedence, falling back to api
	for f := range unionFields {
		if f == "resources" || f == "additionalEnv" {
			continue // already handled
		}
		var val interface{}
		if backendMap != nil {
			if v, ok := backendMap[f]; ok && v != nil {
				val = v
			}
		}
		if val == nil && apiMap != nil {
			if v, ok := apiMap[f]; ok && v != nil {
				val = v
			}
		}
		if val != nil {
			newComponent.Object[f] = val
		}
	}

	slog.Info(componentName + " spec conversion completed")
	return newComponent
}

// convertAppengineApiSpec converts the spec.components.appengineApi section from v1alpha2 to v2alpha1
// AppEngine API is basically unchanged between v1alpha2 and v2alpha1
func convertAppengineApiSpec(oldSpec *unstructured.Unstructured) (newAppengineApi *unstructured.Unstructured) {
	slog.Info("Converting AppEngine API spec")
	newAppengineApi = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldAppengineApi, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", "appengineApi")
	if err != nil {
		slog.Error("error retrieving appengineApi spec", "err", err)
	}
	if !found || oldAppengineApi == nil {
		slog.Warn("spec.components.appengineApi section is missing or empty in the input CR. Resulting CR will have no appengineApi spec.")
		return newAppengineApi
	}

	// The following fields are deep copied from the old appengineApi to the new appengineApi. No changes here
	dc1 := []string{
		"maxResultsLimit",
		"roomEventsQueueName",
		"roomEventsExchangeName",
	}

	dc2 := getAstarteGenericAPISpecFields()

	dc := append(dc1, dc2...)
	for _, f := range dc {
		sourcePath := []string{"spec", "components", "appengineApi", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newAppengineApi, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Info("AppEngine API spec conversion completed")
	return newAppengineApi
}

// convertDashboardSpec converts the spec.components.dashboard section from v1alpha2 to v2alpha1
// Dashboard is basically unchanged between v1alpha2 and v2alpha1
func convertDashboardSpec(oldSpec *unstructured.Unstructured) (newDashboardApi *unstructured.Unstructured) {
	slog.Info("Converting Dashboard spec")
	newDashboardApi = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldDashboardApi, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", "dashboard")
	if err != nil {
		slog.Error("error retrieving dashboard spec", "err", err)
	}
	if !found || oldDashboardApi == nil {
		slog.Warn("spec.components.dashboard section is missing or empty in the input CR. Resulting CR will have no dashboard spec.")
		return newDashboardApi
	}

	// The following fields are deep copied from the old dashboard to the new dashboard. No changes here
	dc1 := []string{
		"realmManagementApiUrl",
		"appEngineApiUrl",
		"pairingApiUrl",
		"flowApiUrl",
		"defaultRealm",
		"defaultAuth",
		"auth",
	}

	dc2 := getAstarteGenericAPISpecFields()

	dc := append(dc1, dc2...)
	for _, f := range dc {
		sourcePath := []string{"spec", "components", "dashboard", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newDashboardApi, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Info("Dashboard spec conversion completed")
	return newDashboardApi
}

// convertFlowSpec converts the spec.components.flow section from v1alpha2 to v2alpha1
// Flow is basically unchanged between v1alpha2 and v2alpha1
func convertFlowSpec(oldSpec *unstructured.Unstructured) (newFlow *unstructured.Unstructured) {
	slog.Info("Converting Flow spec")
	newFlow = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldFlow, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", "flow")
	if err != nil {
		slog.Error("error retrieving flow spec", "err", err)
	}
	if !found || oldFlow == nil {
		slog.Warn("spec.components.flow section is missing or empty in the input CR. Resulting CR will have no flow spec.")
		return newFlow
	}

	// The following fields are deep copied from the old flow to the new flow. No changes here
	dc := getAstarteGenericAPISpecFields()

	for _, f := range dc {
		sourcePath := []string{"spec", "components", "flow", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newFlow, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Info("Flow spec conversion completed")
	return newFlow
}

// convertTriggerEngineSpec converts the spec.components.triggerEngine section from v1alpha2 to v2alpha1
// TE is basically unchanged between v1alpha2 and v2alpha1
func convertTriggerEngineSpec(oldSpec *unstructured.Unstructured) (newTriggerEngine *unstructured.Unstructured) {
	slog.Info("Converting TriggerEngine spec")
	newTriggerEngine = &unstructured.Unstructured{Object: map[string]interface{}{}}
	oldTriggerEngine, found, err := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "components", "triggerEngine")
	if err != nil {
		slog.Error("error retrieving triggerEngine spec", "err", err)
	}
	if !found || oldTriggerEngine == nil {
		slog.Warn("spec.components.triggerEngine section is missing or empty in the input CR. Resulting CR will have no triggerEngine spec.")
		return newTriggerEngine
	}

	// The following fields are deep copied from the old triggerEngine to the new triggerEngine. No changes here
	dc1 := getAstarteGenericClusteredResourceFields()
	dc2 := []string{
		"eventsQueueName",
		"eventsRoutingKey",
	}
	dc := append(dc1, dc2...)
	for _, f := range dc {
		sourcePath := []string{"spec", "components", "triggerEngine", f}
		destPath := []string{f}
		err = migrationutils.CopyIfExists(oldSpec, newTriggerEngine, sourcePath, destPath)
		if err != nil {
			slog.Error("error copying field", "field", f, "err", err)
		}
	}

	slog.Info("TriggerEngine spec conversion completed")
	return newTriggerEngine
}

// convertSpec converts the spec section from v1alpha2 to v2alpha1
func convertSpec(oldSpec *unstructured.Unstructured) (newSpec *unstructured.Unstructured) {

	// Initialize the destination Unstructured with a non-nil Object map
	newSpec = &unstructured.Unstructured{Object: map[string]interface{}{}}

	// Check if spec is nil, if so, return nil
	if oldSpec.Object == nil {
		slog.Error("spec section is missing or empty in the input CR. Resulting CR will have no spec.")
		return nil
	}

	// The following fields are deep copied from the old spec to the new spec. No changes here
	dc := []string{"api", "version", "imagePullPolicy", "imagePullSecrets", "distributionChannel", "deploymentStrategy", "features", "storageClassName", "astarteInstanceID", "manualMaintenanceMode"}
	for _, f := range dc {
		sourcePath := []string{"spec", f}
		// We are building the contents of spec here, so destination must be the root of newSpec
		destPath := []string{f}
		_ = migrationutils.CopyIfExists(oldSpec, newSpec, sourcePath, destPath)
	}

	// Check for and warn about removed fields
	if rbac, found, _ := unstructured.NestedFieldNoCopy(oldSpec.Object, "spec", "rbac"); found && rbac != nil {
		slog.Warn("spec.rbac field is no longer supported and will be ignored. RBAC is now always managed by the operator.")
	}

	// Cassandra: spec.cassandra conversion
	if cass := convertCassandraSpec(oldSpec); cass != nil && len(cass.Object) > 0 {
		unstructured.SetNestedField(newSpec.Object, cass.Object, "cassandra")
	}

	// RabbitMQ: spec.rabbitmq conversion
	if rmq := convertRabbitMQSpec(oldSpec); rmq != nil && len(rmq.Object) > 0 {
		unstructured.SetNestedField(newSpec.Object, rmq.Object, "rabbitmq")
	}

	// VerneMQ: spec.vernemq conversion
	if vmq := convertVernemqSpec(oldSpec); vmq != nil && len(vmq.Object) > 0 {
		unstructured.SetNestedField(newSpec.Object, vmq.Object, "vernemq")
	}

	// CFSSL: spec.cfssl conversion
	if cfssl := convertCfsslSpec(oldSpec); cfssl != nil && len(cfssl.Object) > 0 {
		unstructured.SetNestedField(newSpec.Object, cfssl.Object, "cfssl")
	}

	// Components: spec.components conversion
	if comps := convertComponentsSpec(oldSpec); comps != nil && len(comps.Object) > 0 {
		unstructured.SetNestedField(newSpec.Object, comps.Object, "components")
	}

	return newSpec
}

// v1alpha2toV2alpha1 converts a v1alpha2 Astarte CR to a v2alpha1 CR
func v1alpha2toV2alpha1(oldCr *v1alpha2) (*v2alpha1, error) {

	// Disclamer: this code is very verbose and not very elegant, i know. The goal here
	// is to have a very explicit and clear procedural approach to the conversion, so that
	// we can easily spot what is being converted, what is being ignored and what needs
	// user intervention. This is a one-time use tool, so maintainability and elegance
	// are not a priority here. I swear, I usually write much better code.

	// Initialize the destination Unstructured with a non-nil Object map
	newCr := &v2alpha1{Object: map[string]interface{}{}}

	// If not v1alpha2, return error
	apiVersion, found, err := unstructured.NestedString(oldCr.Object, "apiVersion")
	if err != nil || !found || apiVersion != "api.astarte-platform.org/v1alpha2" {
		return nil, fmt.Errorf("error: input CR is not v1alpha2. Cannot convert to v2alpha1")
	}

	// Update API version
	err = unstructured.SetNestedField(newCr.Object, "api.astarte-platform.org/v2alpha1", "apiVersion")
	if err != nil {
		return nil, fmt.Errorf("error setting apiVersion in new CR: %v", err)
	}

	// Copy kind
	kind, found, err := unstructured.NestedString(oldCr.Object, "kind")
	if err != nil || !found || kind != "Astarte" {
		return nil, fmt.Errorf("error: input CR kind is not Astarte. Cannot convert to v2alpha1")
	}
	err = unstructured.SetNestedField(newCr.Object, kind, "kind")

	// Copy name and namespace from metadata
	name, found, err := unstructured.NestedString(oldCr.Object, "metadata", "name")
	if err != nil || !found {
		return nil, fmt.Errorf("error: input CR metadata.name not found. Cannot convert to v2alpha1")
	}
	err = unstructured.SetNestedField(newCr.Object, name, "metadata", "name")

	// Copy namespace only if found, else set it to "default"
	namespace, found, err := unstructured.NestedString(oldCr.Object, "metadata", "namespace")
	if err != nil {
		return nil, fmt.Errorf("error: input CR metadata.namespace. Cannot convert to v2alpha1")
	}

	err = unstructured.SetNestedField(newCr.Object, "default", "metadata", "namespace")
	if found {
		err = unstructured.SetNestedField(newCr.Object, namespace, "metadata", "namespace")
	}
	if err != nil {
		return nil, fmt.Errorf("error setting metadata.namespace in new CR: %v", err)
	}

	// Assign converted spec to newCr
	convertedSpec := convertSpec(oldCr)
	if convertedSpec != nil {
		// Set the underlying map (convertedSpec.Object) as the spec
		if err = unstructured.SetNestedField(newCr.Object, convertedSpec.Object, "spec"); err != nil {
			return nil, fmt.Errorf("error setting spec in new CR: %v", err)
		}
	} else {
		slog.Warn("spec section is missing or empty in the input CR. Resulting CR will have no spec.")
	}

	return newCr, nil
}

// ConvertToV2alpha1 is the exported entry point to convert a v1alpha2 Astarte CR
// into a v2alpha1 CR. It wraps the internal conversion logic.
func ConvertToV2alpha1(oldCr *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return v1alpha2toV2alpha1(oldCr)
}

// getAstarteGenericClusteredResourceFields returns a list of fields that are common to all Astarte Backend components
func getAstarteGenericClusteredResourceFields() []string {
	return []string{
		"deploy",
		"replicas",
		"antiAffinity",
		"customAffinity",
		"deploymentStrategy",
		"version",
		"image",
		"resources",
		"additionalEnv",
		"podLabels",
		"autoscaler",
		"priorityClass",
	}
}

// getAstarteGenericAPISpecFields returns a list of fields that are common to all Astarte API components
func getAstarteGenericAPISpecFields() []string {
	fields := getAstarteGenericClusteredResourceFields()
	fields = append(fields, []string{
		"disableAuthentication",
	}...)
	return fields
}
