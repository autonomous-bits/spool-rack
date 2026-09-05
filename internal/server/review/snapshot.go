package review

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
)

// SnapshotVersion is the version of the Rack review snapshot contract.
const SnapshotVersion uint32 = 1

// SnapshotEnvelopeVersion is the version of Rack's canonical CBOR object
// envelope. It is deliberately independent of SnapshotVersion: a v1 review
// snapshot is losslessly represented in this v2 storage format.
const SnapshotEnvelopeVersion uint32 = 2

var (
	// ErrInvalidSnapshot indicates malformed, incomplete, or nondeterministic
	// snapshot JSON.
	ErrInvalidSnapshot = errors.New("review: invalid graph snapshot")
	// ErrUnsupportedSnapshotVersion indicates a snapshot from a contract version
	// this server cannot safely interpret.
	ErrUnsupportedSnapshotVersion = errors.New("review: unsupported graph snapshot version")
	// ErrSchemaViolation indicates graph data conflicts with the schema embedded
	// in the same immutable snapshot.
	ErrSchemaViolation = errors.New("review: graph snapshot schema violation")
	// ErrInvalidSnapshotRoot indicates a value that cannot name a CAS object.
	ErrInvalidSnapshotRoot = errors.New("review: invalid snapshot root")
	// ErrInvalidCanonicalCBOR indicates a snapshot object which is not its
	// unique canonical CBOR representation.
	ErrInvalidCanonicalCBOR = errors.New("review: invalid canonical snapshot CBOR")
	// ErrUnsupportedSnapshotData indicates JSON data that cannot be represented
	// by graphcontract without an unsafe conversion.
	ErrUnsupportedSnapshotData = errors.New("review: unsupported snapshot data")
)

var snapshotRootPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var (
	snapshotCanonicalCBOR, _ = cbor.CanonicalEncOptions().EncMode()
	snapshotCBORDecoder, _   = cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		IndefLength:       cbor.IndefLengthForbidden,
		TagsMd:            cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	}.DecMode()
)

// Snapshot is a versioned, immutable graph value stored as one JSON object in
// CAS. Its ordered collections are part of the contract: Nodes and Edges are
// sorted by ID; labels, properties, schema rules, and cardinality rules have
// the ordering validated by Validate. A decoded Snapshot contains no state
// outside this object, so a merge preview can safely load each of its three
// inputs independently by snapshot root.
//
// Properties are an ordered list rather than a map to make duplicate keys and
// iteration order explicit in the on-disk representation. Each property's
// Value is canonical JSON whose shape is declared by Type.
type Snapshot struct {
	Version uint32 `json:"version" cbor:"1,keyasint"`
	Schema  Schema `json:"schema" cbor:"2,keyasint"`
	Nodes   []Node `json:"nodes" cbor:"3,keyasint"`
	Edges   []Edge `json:"edges" cbor:"4,keyasint"`
}

// Node is a labeled graph vertex.
type Node struct {
	ID         string     `json:"id" cbor:"1,keyasint"`
	Labels     []string   `json:"labels" cbor:"2,keyasint"`
	Properties []Property `json:"properties" cbor:"3,keyasint"`
}

// Edge is a labeled directed graph relationship from From to To.
type Edge struct {
	ID         string     `json:"id" cbor:"1,keyasint"`
	From       string     `json:"from" cbor:"2,keyasint"`
	To         string     `json:"to" cbor:"3,keyasint"`
	Labels     []string   `json:"labels" cbor:"4,keyasint"`
	Properties []Property `json:"properties" cbor:"5,keyasint"`
}

// Property is one typed JSON property. Value must be canonical JSON and its
// JSON type must exactly match Type.
type Property struct {
	Name  string          `json:"name" cbor:"1,keyasint"`
	Type  ValueType       `json:"type" cbor:"2,keyasint"`
	Value json.RawMessage `json:"value" cbor:"3,keyasint"`
}

// ValueType is the JSON shape allowed for a Property value.
type ValueType string

const (
	ValueTypeNull    ValueType = "null"
	ValueTypeBoolean ValueType = "boolean"
	ValueTypeNumber  ValueType = "number"
	ValueTypeString  ValueType = "string"
	ValueTypeArray   ValueType = "array"
	ValueTypeObject  ValueType = "object"
)

// Schema supplies the type and cardinality facts a three-way merge preview
// needs to decide whether an otherwise structural merge is valid.
type Schema struct {
	NodeLabels    []LabelRule       `json:"nodeLabels" cbor:"1,keyasint"`
	EdgeLabels    []LabelRule       `json:"edgeLabels" cbor:"2,keyasint"`
	Cardinalities []CardinalityRule `json:"cardinalities" cbor:"3,keyasint"`
}

// LabelRule constrains properties for every node or edge bearing Label.
// Properties not mentioned by a matching rule remain valid, preserving the
// graph's schemaless property capability.
type LabelRule struct {
	Label      string         `json:"label" cbor:"1,keyasint"`
	Properties []PropertyRule `json:"properties" cbor:"2,keyasint"`
}

// PropertyRule constrains a named property when its enclosing Label applies.
type PropertyRule struct {
	Name     string    `json:"name" cbor:"1,keyasint"`
	Type     ValueType `json:"type" cbor:"2,keyasint"`
	Required bool      `json:"required" cbor:"3,keyasint"`
}

// CardinalityRule applies to each node with FromLabel. Its outgoing edges
// bearing EdgeLabel and ending at a node with ToLabel must be between Min and
// Max inclusive. A nil Max is unbounded.
type CardinalityRule struct {
	EdgeLabel string `json:"edgeLabel" cbor:"1,keyasint"`
	FromLabel string `json:"fromLabel" cbor:"2,keyasint"`
	ToLabel   string `json:"toLabel" cbor:"3,keyasint"`
	Min       int    `json:"min" cbor:"4,keyasint"`
	Max       *int   `json:"max,omitempty" cbor:"5,keyasint,omitempty"`
}

// SnapshotObjectStore is the minimal CAS capability needed to decode a graph
// snapshot. cas.Driver satisfies this interface; the narrow shape keeps the
// decoder reusable by merge preview code and lightweight tests.
type SnapshotObjectStore interface {
	Get(ctx context.Context, scope cas.Scope, hash string) ([]byte, error)
}

// SnapshotDecoder loads and validates immutable graph snapshots from CAS.
type SnapshotDecoder struct {
	objects SnapshotObjectStore
}

// NewSnapshotDecoder constructs a CAS-backed snapshot decoder.
func NewSnapshotDecoder(objects SnapshotObjectStore) *SnapshotDecoder {
	return &SnapshotDecoder{objects: objects}
}

// Decode loads snapshotRoot from the supplied tenant/repository CAS scope and
// strictly decodes its graph snapshot contract.
func (d *SnapshotDecoder) Decode(ctx context.Context, scope cas.Scope, snapshotRoot string) (Snapshot, error) {
	if d == nil || d.objects == nil {
		return Snapshot{}, fmt.Errorf("%w: object store is required", ErrInvalidSnapshot)
	}
	return DecodeSnapshot(ctx, d.objects, scope, snapshotRoot)
}

// DecodeSnapshot loads a versioned graph snapshot directly from a CAS-backed
// object store. Errors from the underlying object store remain wrapped, so
// callers can use errors.Is(err, cas.ErrNotFound) and related CAS sentinels.
func DecodeSnapshot(ctx context.Context, objects SnapshotObjectStore, scope cas.Scope, snapshotRoot string) (Snapshot, error) {
	if !snapshotRootPattern.MatchString(snapshotRoot) {
		return Snapshot{}, fmt.Errorf("%w: expected 64 lowercase hexadecimal characters", ErrInvalidSnapshotRoot)
	}
	if objects == nil {
		return Snapshot{}, fmt.Errorf("%w: object store is required", ErrInvalidSnapshot)
	}
	data, err := objects.Get(ctx, scope, snapshotRoot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("review: load snapshot root %s: %w", snapshotRoot, err)
	}
	snapshot, err := DecodeSnapshotObject(data)
	if err != nil {
		return Snapshot{}, fmt.Errorf("review: decode snapshot root %s: %w", snapshotRoot, err)
	}
	return snapshot, nil
}

// DecodeSnapshotObject decodes a stored review snapshot. JSON is accepted
// only as the migration bridge for immutable v1 objects; new objects must use
// DecodeSnapshotCBOR's canonical v2 envelope.
func DecodeSnapshotObject(data []byte) (Snapshot, error) {
	if len(bytes.TrimSpace(data)) > 0 && bytes.TrimSpace(data)[0] == '{' {
		return DecodeSnapshotJSON(data)
	}
	return DecodeSnapshotCBOR(data)
}

// DecodeSnapshotJSON strictly decodes and validates the JSON payload of a
// snapshot object. Unknown fields, duplicate object members, trailing data,
// unsupported versions, and noncanonical collection order are rejected.
func DecodeSnapshotJSON(data []byte) (Snapshot, error) {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}

	var wire snapshotWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Snapshot{}, fmt.Errorf("%w: decode JSON: %w", ErrInvalidSnapshot, err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	if wire.Version == nil || wire.Schema == nil || wire.Nodes == nil || wire.Edges == nil {
		return Snapshot{}, fmt.Errorf("%w: version, schema, nodes, and edges are required", ErrInvalidSnapshot)
	}

	snapshot := Snapshot{
		Version: *wire.Version,
		Schema:  *wire.Schema,
		Nodes:   *wire.Nodes,
		Edges:   *wire.Edges,
	}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// MarshalSnapshotJSON validates snapshot and returns its deterministic JSON
// representation for placement in a CAS object. It does not write the object:
// callers retain control of the CAS scope and the content hash used as its
// snapshotRoot.
func MarshalSnapshotJSON(snapshot Snapshot) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: encode JSON: %w", ErrInvalidSnapshot, err)
	}
	return data, nil
}

// MarshalSnapshotCBOR validates snapshot and writes Rack's v2 canonical CBOR
// envelope. The envelope contains the complete Rack model, including raw JSON
// property values, as well as graphcontract's canonical graph objects. The
// Rack model remains authoritative so decimal/exponent spelling is never
// recovered from a float or otherwise silently coerced.
func MarshalSnapshotCBOR(snapshot Snapshot) ([]byte, error) {
	envelope, err := newSnapshotEnvelope(snapshot)
	if err != nil {
		return nil, err
	}
	data, err := snapshotCanonicalCBOR.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("%w: encode CBOR: %v", ErrInvalidSnapshot, err)
	}
	return data, nil
}

// DecodeSnapshotCBOR strictly decodes a v2 snapshot envelope. Re-marshalling
// and comparing the input rejects alternate integer widths, map ordering,
// duplicate keys, indefinite lengths, and every other noncanonical encoding.
func DecodeSnapshotCBOR(data []byte) (Snapshot, error) {
	var envelope snapshotEnvelope
	if err := snapshotCBORDecoder.Unmarshal(data, &envelope); err != nil {
		return Snapshot{}, fmt.Errorf("%w: decode CBOR: %v", ErrInvalidCanonicalCBOR, err)
	}
	if envelope.Version != SnapshotEnvelopeVersion {
		return Snapshot{}, fmt.Errorf("%w: envelope version %d", ErrUnsupportedSnapshotVersion, envelope.Version)
	}
	canonical, err := snapshotCanonicalCBOR.Marshal(envelope)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: re-encode CBOR: %v", ErrInvalidCanonicalCBOR, err)
	}
	if !bytes.Equal(data, canonical) {
		return Snapshot{}, fmt.Errorf("%w: object is not canonically encoded", ErrInvalidCanonicalCBOR)
	}
	if err := envelope.validate(); err != nil {
		return Snapshot{}, err
	}
	return envelope.Snapshot, nil
}

// MarshalSnapshot is the storage codec for newly-created Rack snapshots.
func MarshalSnapshot(snapshot Snapshot) ([]byte, error) {
	return MarshalSnapshotCBOR(snapshot)
}

type snapshotEnvelope struct {
	Version  uint32               `cbor:"1,keyasint"`
	Snapshot Snapshot             `cbor:"2,keyasint"`
	Nodes    []graphcontract.Node `cbor:"3,keyasint"`
	Edges    []graphcontract.Edge `cbor:"4,keyasint"`
}

func newSnapshotEnvelope(snapshot Snapshot) (snapshotEnvelope, error) {
	if err := snapshot.Validate(); err != nil {
		return snapshotEnvelope{}, err
	}
	envelope := snapshotEnvelope{
		Version:  SnapshotEnvelopeVersion,
		Snapshot: snapshot,
		Nodes:    make([]graphcontract.Node, len(snapshot.Nodes)),
		Edges:    make([]graphcontract.Edge, len(snapshot.Edges)),
	}
	for i, node := range snapshot.Nodes {
		properties, err := graphProperties(node.Properties)
		if err != nil {
			return snapshotEnvelope{}, fmt.Errorf("%w: node %q: %v", ErrUnsupportedSnapshotData, node.ID, err)
		}
		graphNode, err := graphcontract.NewNode(node.ID, "", node.Labels, properties)
		if err != nil {
			return snapshotEnvelope{}, fmt.Errorf("%w: node %q: %v", ErrUnsupportedSnapshotData, node.ID, err)
		}
		envelope.Nodes[i] = graphNode
	}
	for i, edge := range snapshot.Edges {
		properties, err := graphProperties(edge.Properties)
		if err != nil {
			return snapshotEnvelope{}, fmt.Errorf("%w: edge %q: %v", ErrUnsupportedSnapshotData, edge.ID, err)
		}
		// graphcontract has one edge Type while Rack preserves its complete,
		// ordered label set in Snapshot.Edges. The first label is a stable
		// projection, never a replacement for the Rack-owned labels.
		graphEdge, err := graphcontract.NewEdge(edge.ID, edge.From, edge.To, edge.Labels[0], properties)
		if err != nil {
			return snapshotEnvelope{}, fmt.Errorf("%w: edge %q: %v", ErrUnsupportedSnapshotData, edge.ID, err)
		}
		envelope.Edges[i] = graphEdge
	}
	return envelope, nil
}

func (e snapshotEnvelope) validate() error {
	expected, err := newSnapshotEnvelope(e.Snapshot)
	if err != nil {
		return err
	}
	if len(e.Nodes) != len(expected.Nodes) || len(e.Edges) != len(expected.Edges) {
		return fmt.Errorf("%w: graphcontract object count does not match Rack snapshot", ErrInvalidSnapshot)
	}
	for i := range e.Nodes {
		if !e.Nodes[i].Equal(expected.Nodes[i]) {
			return fmt.Errorf("%w: graphcontract node %d does not match Rack snapshot", ErrInvalidSnapshot, i)
		}
	}
	for i := range e.Edges {
		if !e.Edges[i].Equal(expected.Edges[i]) {
			return fmt.Errorf("%w: graphcontract edge %d does not match Rack snapshot", ErrInvalidSnapshot, i)
		}
	}
	return nil
}

func graphProperties(properties []Property) (map[string]graphcontract.PropertyValue, error) {
	result := make(map[string]graphcontract.PropertyValue, len(properties))
	for _, property := range properties {
		value, err := graphPropertyValue(property.Value)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", property.Name, err)
		}
		result[property.Name] = value
	}
	return result, nil
}

func graphPropertyValue(raw json.RawMessage) (graphcontract.PropertyValue, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return graphcontract.PropertyValue{}, fmt.Errorf("decode JSON: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return graphcontract.PropertyValue{}, err
	}
	return graphValue(value)
}

func graphValue(value any) (graphcontract.PropertyValue, error) {
	switch value := value.(type) {
	case nil:
		return graphcontract.NullPropertyValue(), nil
	case bool:
		return graphcontract.BoolPropertyValue(value), nil
	case string:
		return graphcontract.StringPropertyValue(value), nil
	case json.Number:
		if !strings.ContainsAny(value.String(), ".eE") {
			integer, err := value.Int64()
			if err != nil {
				return graphcontract.PropertyValue{}, fmt.Errorf("integer %q cannot be represented by graphcontract", value)
			}
			return graphcontract.IntegerPropertyValue(integer), nil
		}
		number, err := strconv.ParseFloat(value.String(), 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return graphcontract.PropertyValue{}, fmt.Errorf("number %q cannot be represented by graphcontract", value)
		}
		return graphcontract.FloatPropertyValue(number), nil
	case []any:
		items := make([]graphcontract.PropertyValue, len(value))
		for i, item := range value {
			converted, err := graphValue(item)
			if err != nil {
				return graphcontract.PropertyValue{}, fmt.Errorf("array item %d: %w", i, err)
			}
			items[i] = converted
		}
		return graphcontract.ListPropertyValue(items), nil
	case map[string]any:
		items := make(map[string]graphcontract.PropertyValue, len(value))
		for key, item := range value {
			converted, err := graphValue(item)
			if err != nil {
				return graphcontract.PropertyValue{}, fmt.Errorf("object key %q: %w", key, err)
			}
			items[key] = converted
		}
		return graphcontract.MapPropertyValue(items), nil
	default:
		return graphcontract.PropertyValue{}, fmt.Errorf("unexpected JSON value type %T", value)
	}
}

type snapshotWire struct {
	Version *uint32 `json:"version"`
	Schema  *Schema `json:"schema"`
	Nodes   *[]Node `json:"nodes"`
	Edges   *[]Edge `json:"edges"`
}

// Validate verifies all structural, typed-property, schema, and cardinality
// invariants without modifying the snapshot.
func (s Snapshot) Validate() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("%w: version %d", ErrUnsupportedSnapshotVersion, s.Version)
	}
	if s.Nodes == nil || s.Edges == nil {
		return invalidSnapshotf("nodes and edges must be arrays")
	}
	if err := validateSchema(s.Schema); err != nil {
		return err
	}

	nodes := make(map[string]Node, len(s.Nodes))
	for i, node := range s.Nodes {
		if i > 0 && s.Nodes[i-1].ID >= node.ID {
			return invalidSnapshotf("nodes must be strictly ordered by ID")
		}
		if err := validateNode(node); err != nil {
			return fmt.Errorf("%w: node %q: %v", ErrInvalidSnapshot, node.ID, err)
		}
		nodes[node.ID] = node
	}

	for i, edge := range s.Edges {
		if i > 0 && s.Edges[i-1].ID >= edge.ID {
			return invalidSnapshotf("edges must be strictly ordered by ID")
		}
		if err := validateEdge(edge); err != nil {
			return fmt.Errorf("%w: edge %q: %v", ErrInvalidSnapshot, edge.ID, err)
		}
		if _, ok := nodes[edge.From]; !ok {
			return invalidSnapshotf("edge %q refers to missing source node %q", edge.ID, edge.From)
		}
		if _, ok := nodes[edge.To]; !ok {
			return invalidSnapshotf("edge %q refers to missing destination node %q", edge.ID, edge.To)
		}
	}

	if err := validatePropertyRules(s.Schema, s.Nodes, s.Edges); err != nil {
		return err
	}
	if err := validateCardinalities(s.Schema.Cardinalities, s.Nodes, s.Edges, nodes); err != nil {
		return err
	}
	return nil
}

func validateNode(node Node) error {
	if node.ID == "" {
		return errors.New("ID is required")
	}
	if node.Properties == nil {
		return errors.New("properties must be an array")
	}
	if err := validateLabels(node.Labels); err != nil {
		return err
	}
	return validateProperties(node.Properties)
}

func validateEdge(edge Edge) error {
	if edge.ID == "" {
		return errors.New("ID is required")
	}
	if edge.From == "" || edge.To == "" {
		return errors.New("from and to are required")
	}
	if edge.Properties == nil {
		return errors.New("properties must be an array")
	}
	if err := validateLabels(edge.Labels); err != nil {
		return err
	}
	return validateProperties(edge.Properties)
}

func validateSchema(schema Schema) error {
	if schema.NodeLabels == nil || schema.EdgeLabels == nil || schema.Cardinalities == nil {
		return invalidSnapshotf("schema collections must be arrays")
	}
	if err := validateLabelRules("nodeLabels", schema.NodeLabels); err != nil {
		return err
	}
	if err := validateLabelRules("edgeLabels", schema.EdgeLabels); err != nil {
		return err
	}
	for i, rule := range schema.Cardinalities {
		if i > 0 && compareCardinality(schema.Cardinalities[i-1], rule) >= 0 {
			return invalidSnapshotf("cardinalities must be strictly ordered by edgeLabel, fromLabel, and toLabel")
		}
		if rule.EdgeLabel == "" || rule.FromLabel == "" || rule.ToLabel == "" {
			return invalidSnapshotf("cardinality %d requires edgeLabel, fromLabel, and toLabel", i)
		}
		if rule.Min < 0 {
			return invalidSnapshotf("cardinality %d has negative min", i)
		}
		if rule.Max != nil && (*rule.Max < rule.Min || *rule.Max < 0) {
			return invalidSnapshotf("cardinality %d has max smaller than min", i)
		}
	}
	return nil
}

func validateLabelRules(kind string, rules []LabelRule) error {
	for i, rule := range rules {
		if i > 0 && rules[i-1].Label >= rule.Label {
			return invalidSnapshotf("%s must be strictly ordered by label", kind)
		}
		if rule.Label == "" {
			return invalidSnapshotf("%s rule %d requires a label", kind, i)
		}
		if rule.Properties == nil {
			return invalidSnapshotf("%s rule %q properties must be an array", kind, rule.Label)
		}
		for j, property := range rule.Properties {
			if j > 0 && rule.Properties[j-1].Name >= property.Name {
				return invalidSnapshotf("%s rule %q properties must be strictly ordered by name", kind, rule.Label)
			}
			if property.Name == "" || !validValueType(property.Type) {
				return invalidSnapshotf("%s rule %q has invalid property rule", kind, rule.Label)
			}
		}
	}
	return nil
}

func validateLabels(labels []string) error {
	if len(labels) == 0 {
		return errors.New("at least one label is required")
	}
	for i, label := range labels {
		if label == "" {
			return errors.New("labels must not be empty")
		}
		if i > 0 && labels[i-1] >= label {
			return errors.New("labels must be strictly ordered")
		}
	}
	return nil
}

func validateProperties(properties []Property) error {
	for i, property := range properties {
		if property.Name == "" || !validValueType(property.Type) {
			return fmt.Errorf("property %d has an invalid name or type", i)
		}
		if i > 0 && properties[i-1].Name >= property.Name {
			return errors.New("properties must be strictly ordered by name")
		}
		if err := validatePropertyValue(property); err != nil {
			return fmt.Errorf("property %q: %w", property.Name, err)
		}
	}
	return nil
}

func validatePropertyValue(property Property) error {
	if len(property.Value) == 0 {
		return errors.New("value is required")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(property.Value))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("invalid JSON value: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("canonicalize value: %w", err)
	}
	if !bytes.Equal(canonical, property.Value) {
		return errors.New("value must use canonical JSON")
	}
	switch property.Type {
	case ValueTypeNull:
		if value != nil {
			return errors.New("type null requires a null value")
		}
	case ValueTypeBoolean:
		if _, ok := value.(bool); !ok {
			return errors.New("type boolean requires a boolean value")
		}
	case ValueTypeNumber:
		if _, ok := value.(json.Number); !ok {
			return errors.New("type number requires a number value")
		}
	case ValueTypeString:
		if _, ok := value.(string); !ok {
			return errors.New("type string requires a string value")
		}
	case ValueTypeArray:
		if _, ok := value.([]any); !ok {
			return errors.New("type array requires an array value")
		}
	case ValueTypeObject:
		if _, ok := value.(map[string]any); !ok {
			return errors.New("type object requires an object value")
		}
	}
	return nil
}

func validatePropertyRules(schema Schema, nodes []Node, edges []Edge) error {
	nodeRules := labelRuleMap(schema.NodeLabels)
	for _, node := range nodes {
		if err := validateElementPropertyRules("node", node.ID, node.Labels, node.Properties, nodeRules); err != nil {
			return err
		}
	}
	edgeRules := labelRuleMap(schema.EdgeLabels)
	for _, edge := range edges {
		if err := validateElementPropertyRules("edge", edge.ID, edge.Labels, edge.Properties, edgeRules); err != nil {
			return err
		}
	}
	return nil
}

func labelRuleMap(rules []LabelRule) map[string]LabelRule {
	result := make(map[string]LabelRule, len(rules))
	for _, rule := range rules {
		result[rule.Label] = rule
	}
	return result
}

func validateElementPropertyRules(kind, id string, labels []string, properties []Property, rules map[string]LabelRule) error {
	actual := make(map[string]Property, len(properties))
	for _, property := range properties {
		actual[property.Name] = property
	}
	expected := make(map[string]PropertyRule)
	for _, label := range labels {
		rule, ok := rules[label]
		if !ok {
			continue
		}
		for _, propertyRule := range rule.Properties {
			if existing, exists := expected[propertyRule.Name]; exists && existing.Type != propertyRule.Type {
				return schemaViolationf("%s %q labels specify incompatible types for property %q", kind, id, propertyRule.Name)
			} else if !exists || propertyRule.Required {
				expected[propertyRule.Name] = propertyRule
			}
		}
	}
	for name, rule := range expected {
		property, exists := actual[name]
		if !exists {
			if rule.Required {
				return schemaViolationf("%s %q is missing required property %q", kind, id, name)
			}
			continue
		}
		if property.Type != rule.Type {
			return schemaViolationf("%s %q property %q has type %q, want %q", kind, id, name, property.Type, rule.Type)
		}
	}
	return nil
}

func validateCardinalities(rules []CardinalityRule, nodes []Node, edges []Edge, byID map[string]Node) error {
	for _, rule := range rules {
		for _, node := range nodes {
			if !hasLabel(node.Labels, rule.FromLabel) {
				continue
			}
			count := 0
			for _, edge := range edges {
				if edge.From == node.ID && hasLabel(edge.Labels, rule.EdgeLabel) && hasLabel(byID[edge.To].Labels, rule.ToLabel) {
					count++
				}
			}
			if count < rule.Min {
				return schemaViolationf("node %q has %d %q edges to %q nodes, below minimum %d", node.ID, count, rule.EdgeLabel, rule.ToLabel, rule.Min)
			}
			if rule.Max != nil && count > *rule.Max {
				return schemaViolationf("node %q has %d %q edges to %q nodes, above maximum %d", node.ID, count, rule.EdgeLabel, rule.ToLabel, *rule.Max)
			}
		}
	}
	return nil
}

func hasLabel(labels []string, want string) bool {
	index := sort.SearchStrings(labels, want)
	return index < len(labels) && labels[index] == want
}

func validValueType(valueType ValueType) bool {
	switch valueType {
	case ValueTypeNull, ValueTypeBoolean, ValueTypeNumber, ValueTypeString, ValueTypeArray, ValueTypeObject:
		return true
	default:
		return false
	}
}

func compareCardinality(left, right CardinalityRule) int {
	if left.EdgeLabel != right.EdgeLabel {
		if left.EdgeLabel < right.EdgeLabel {
			return -1
		}
		return 1
	}
	if left.FromLabel != right.FromLabel {
		if left.FromLabel < right.FromLabel {
			return -1
		}
		return 1
	}
	if left.ToLabel < right.ToLabel {
		return -1
	}
	if left.ToLabel > right.ToLabel {
		return 1
	}
	return 0
}

func invalidSnapshotf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSnapshot, fmt.Sprintf(format, args...))
}

func schemaViolationf(format string, args ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrInvalidSnapshot, ErrSchemaViolation, fmt.Sprintf(format, args...))
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return fmt.Errorf("read trailing JSON: %w", err)
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := inspectJSONValue(decoder); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

func inspectJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if _, exists := keys[key]; exists {
					return fmt.Errorf("duplicate object member %q", key)
				}
				keys[key] = struct{}{}
				if err := inspectJSONValue(decoder); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := inspectJSONValue(decoder); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected delimiter %q", delimiter)
		}
	}
	return nil
}
