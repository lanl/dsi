# Denodo Data Catalog → DSI Data Contract v1.0 (Draft, rev 10)

**Status:** Draft — pending Divya's sign-off on two decision points (Section 2.3, 2.5). Decision Point 1 (Section 2.3) is settled as Option A for v1.0; reopening is available on request, not a blocking dependency.
**Scope:** Phase 1 (read-only) backend, `denodo.py`, dev environment (canonical host `datacatalog-d.lanl.gov`, VDB `dataportal`). The 2026-07-27 temporary-host note is superseded: on 2026-07-28 the `datacatalog-d.lanl.gov` alias was observed serving the OAuth redirect JSP again during the PROD re-run — the alias move appears complete. Hostname remains an operational detail, not a contract change.
**Traceability:** Every design choice below is grounded in Step 0's empirical probe of 15,007 active views (~99.9% coverage; 12 `vlanl*task` views consistently returned HTTP 500). **Rev 10 reframe:** Step 0's content statistics (15,007 / 544 / ~5.5% / 12-view-500-cohort) describe the **pre-migration dev snapshot**, which no longer exists on dev (see rev 10 changelog a). The *API semantics* those statistics motivated — composite identity, nullability, EAV, three-map union, normalization — have since been re-verified on post-migration dev and on PROD and are unchanged.

**Changelog rev 10 (2026-07-28, evening):** (a) **Dev environment re-seeded during migration** — the fixture-fetch run against restored `datacatalog-d` found **4,418 views** (vs. 15,014 pre-migration), matching PROD's known count: dev now carries prod-equivalent content. The Step 0 dataset survives only in `probe_results.db` and committed artifacts. Consequences handled in (b)–(e). New **Maxen question M7**: was dev re-seeded from prod in the migration; does the pre-migration 15k dataset survive anywhere; and which environment should DSI treat as canonical dev going forward? (b) **The HTTP-500 cohort no longer reproduces**: post-migration dev lists **30** `vlanl*task` views (a different, prod-origin set vs. the pre-migration 12) and *all return 200*. Section 4.1's `fetch_status='error'` policy is **retained as defensive design** — transient/systematic fetch failures remain possible and must stay queryable — but its empirical anchor is relabeled historical. The `error_500` golden fixture becomes a **synthetic** fixture (`_synthetic: true`, shape-identical to a real error record) until a live failure is observed again. (c) **First empirical second VDB observed**: `ops_core_publication` (4 views, all HTTP 200) appears alongside `dataportal` on post-migration dev — the first real-data confirmation of Design Principle #5 and the composite key (until now grounded in API semantics and Maxen testimony only; see rev 9 d). No cross-database duplicate *name* observed yet (Section 4.3 unchanged). (d) **Fixture set expanded to 8**: `plain` re-selected as `inventory_daily_balance_fact` (original `access_area_type` returns 404 on post-migration dev — absent from the current list; re-selection intersected the probe's zero-signal views with the live list and verified HTTP 200); new **`second_vdb`** fixture (`ops_core_publication.i_ods_pa_cpnt_evthst`) proves non-`dataportal` handling end-to-end. (e) **Fixture provenance rule**: every fixture records `_source_env` and fetch date; fixtures fetched from post-migration dev represent prod-equivalent content and are labeled as such in the fixture README.

**Changelog rev 9 (2026-07-28):** (a) **V1 generalization check CLOSED on PROD** — the re-run (`test_additional_properties_tab_v12_PROD.ipynb`, host `datacatalog.lanl.gov`) enumerated *every* assigned element of *every* non-Summary property group on PROD (4 assignments total across 43 groups) and found every populated non-Summary value (1 total) present in `view-details` under `customTabPropertyMap`, `gap_confirmed=false`. The completeness claim is therefore verified on PROD **by exhaustion, not by sampling**; the "single sampled view" limitation in Section 2.5 is lifted. (b) **`placeToShow` vocabulary completed and mapped**: PROD exposes all three values — `SUMMARY_TAB` ↔ `summaryPropertyMap`, `SPECIFIC_CUSTOM_PROPERTY_TAB` ↔ `customTabPropertyMap`, `GENERAL_CUSTOM_PROPERTY_TAB` ↔ `generalTabPropertyMap`. `generalTabPropertyMap`'s persistent emptiness is now explained: **zero elements are assigned to any GENERAL-tab group anywhere on PROD** — a data state, not a defect. (c) **DCAT ecosystem observed**: 36 of PROD's 43 groups are DCAT-related (W3C Data Catalog Vocabulary), all currently unassigned/unpopulated — evidence of a staged, not-yet-started standards-based metadata rollout; recorded in Section 2.5 as forward-drift evidence for the EAV decision. New **Maxen question M6**: explain the 9-vs-43 group discrepancy between the `-b` test host and PROD (it bounds any coverage claim made from dev/test), and what is the DCAT rollout plan/timeline? (d) **Editorial correction — composite-key justification (Design Principle #2, Section 4.3)**: the prior wording claimed the Step 0 probe *observed* duplicate view names across databases; the probe covered a single VDB (`dataportal`), so no such observation exists. The corrected justification (API-inherent identity, per-VDB namespacing, Maxen-confirmed additional VDBs) is now stated; the composite-key decision itself is unchanged. (e) **Section 5 completeness**: added previously missing source mappings for `denodo_databases.description` (new Section 5.4) and `server_id` (Section 5.3). (f) PROD environment confirmations: `serverId=1` unchanged post-migration; `viewName`/`databaseName` params accepted on PROD as expected.

**Changelog rev 8 (2026-07-27):** (a) Additional-Properties-Tab open item RESOLVED on a sampled view — non-Summary property values ARE present in `view-details` (`customTabPropertyMap`); (b) corrected the property-map item key: `propertyName`, **not** `name` — audit any code using the old key; (c) documented the full shared item schema of the property maps (16 keys incl. embedded `groupName`/`placeToShowGroup`); (d) `connectionUris` upgraded from "possibly boilerplate" to "confirmed per-view API resource with non-routable `localhost` hostnames" — still out of scope for v1.0, now with evidence and a named unblock condition (Maxen question M1); (e) canonical `property_name` construction defined as `groupName + "/" + propertyName` from the item fields.

---

## 1. Design Principles

These principles apply across every table in this contract; individual table specs (Section 2) do not restate them.

1. **Shape.** All data is exchanged as `OrderedDict[table_name → OrderedDict[column_name → list]]`, matching the DSI backend convention used by the reference SQLite/DuckDB backends. Column order within a table is fixed by this document.
2. **Key.** Every table is keyed on the composite `(view_name, db_name)` — or an extension of it (see `denodo_columns`, `denodo_properties`). This is not a stylistic choice, on three grounds *(justification corrected in rev 9 — the prior wording overstated Step 0 evidence; the probe covered a single VDB and observed no cross-database duplicates)*: (a) view identity in Denodo is inherently the pair — the `view-details` endpoint itself requires both `viewName` and `databaseName`; (b) Denodo namespaces views per VDB, so the same view name can legally recur across VDBs; (c) Maxen has confirmed additional VDBs exist beyond `dataportal` (Principle #5), so a `view_name`-only key would break the moment a second VDB onboards.
3. **Types.** Primitives only — `str`, `int`, `float`, `None`. No nested structures, no dates/datetimes as native objects. Timestamps are ISO-8601 strings (e.g. `"2026-07-22T14:03:11Z"`).
4. **Nullable by design.** Only ~5.5% of views carry any resource signal (see Section 2.4). Resource-related columns default to `None` and every downstream consumer (backend, tests, DSI `find()`) must treat `None` as a valid, expected value — not an error state.
5. **No hard-coded databases.** `dataportal` is a *value* that appears in `db_name`, never a constant baked into code paths. Maxen has confirmed other VDBs exist under other roles; the schema must not assume a single database. *(Rev 10: empirically confirmed — `ops_core_publication` observed alongside `dataportal` on post-migration dev, 4 views, all fetchable; see the `second_vdb` fixture.)*
6. **Provenance columns.** Every table carries `fetched_at` (ISO-8601 string) and `source_env` (`"dev"` or `"prod"`), so any exported snapshot is self-describing without external metadata.

---

## 2. Table Specifications

### 2.1 `denodo_databases` (Layer 1 — VDB)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `db_name` | str | No | **Key.** e.g. `"dataportal"`. |
| `description` | str | Yes | |
| `server_id` | int | No | |
| `view_count` | int | No | Denormalized count, refreshed on each fetch. |
| `fetched_at` | str | No | Provenance. |
| `source_env` | str | No | Provenance. |

Trivial today (one row in dev), designed to hold many rows once additional VDBs are onboarded.

### 2.2 `denodo_views` (Layer 2 — dataset)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `view_name` | str | No | **Key (part 1).** |
| `db_name` | str | No | **Key (part 2).** |
| `description` | str | Yes | HTML-stripped (see Section 3.2). |
| `source_system` | str | Yes | Reclassified from `ODS DB Link`. Oracle db-link identifiers (e.g. `EBS_LINK`). `"NOLINK"` → `None`. |
| `access_instructions` | str | Yes | Reclassified from `Access Role Request`. ~93% are `accessit.lanl.gov` URLs (see Section 3.3). |
| `documentation_url` | str | Yes | Scalar, not a list — justified by the finding that all 544 genuine documentation URLs occur **exactly once** per view (see Section 2.3, Decision 1). |
| `fetch_status` | str | No | `"ok"` or `"error"`. See Section 4.1. |
| `fetched_at` | str | No | Provenance. |
| `source_env` | str | No | Provenance. |

**Note on tags/category:** the original Work item B plan mentioned "tags/category if exposed" as part of core catalog metadata. No Denodo field was confirmed during Step 0 to reliably serve this role — `path` (list endpoint) is the closest candidate, but it's a folder path, not a curated tag/category, and no consumer need was identified for it (see Section 5.1). Deferred to v1.1; not an oversight.

**Note on multi-URL properties (probe evidence, 2026-07-22):** the 1:1 scalar shape of `documentation_url` holds specifically for the `description` field (544 URLs / 544 views). Separately, some *properties* can carry multiple URLs in one value — e.g. the most signal-rich view found (`ben_prtn_elig_prfl_f`, 6 signals) has an `AI Portal/A.I. Summary` property containing three URLs (etrm.live, docs.oracle.com, apexapps.oracle.com). This does not weaken the scalar decision: those URLs live in property values and flow into `denodo_properties.property_value` as-is, not into `documentation_url`. Recorded here so the scalar rationale isn't misread as "no view ever has multiple URLs anywhere."

### 2.3 `denodo_resources` (Layer 3 — additional resources) — **Decision Point 1**

Step 0 left this table nearly empty after reclassification: the only survivor is a scalar `documentation_url`, which has a strict 1:1 relationship with its view (never more than one per view in the data observed to date).

**Option A — Fold (recommended for v1.0).** Move `source_system`, `access_instructions`, and `documentation_url` onto `denodo_views` (as specced in 2.2) and drop this table entirely for v1.0.
- *Rationale:* the relationship is 1:1, not 1:many, so a separate table only adds an unnecessary join. Sparsity (94.5% `None`) is handled fine by a nullable column and doesn't by itself justify a separate table — that argument only holds where the attribute count is also large/drifting (see `denodo_properties`, 2.5).
- Layer 3 still exists *conceptually* — this is a physical-representation choice, not an abandonment of the tri-layer model Divya approved.

**Option B — Keep.** Retain a sparse `denodo_resources(view_name, db_name, resource_type, uri, source_field)` to preserve the tri-layer shape literally and future-proof for structured attachments.

**Open item:** structured, genuinely 1:many attachments (multiple files/links per view) would invalidate the 1:1 assumption above and make Option B necessary. Nothing currently in evidence suggests this; no walkthrough or external input is a precondition to freezing this decision. If such a case turns up later, reopening this table is additive and non-breaking — flag it when it comes up and this section gets revised then.

**Evidence update (2026-07-27):** the first genuine 1:many Layer-3 content has now been identified — `connectionUris` (6 connection endpoints per view: JDBC, ODBC ×2, REST, OData, GraphQL), confirmed present in every sampled `view-details` response (see Section 5.2). It does **not** flip the decision for v1.0 because the returned hostnames are `localhost` and therefore not usable by consumers as returned (Maxen question M1: where is the hostname configured, and can it be a routable FQDN?). If M1 resolves favorably, `denodo_resources` in exactly the Option B shape (`view_name, db_name, resource_type, uri, source_field`) is the natural landing table — an additive, non-breaking reopen, which is precisely the escape hatch this section reserved.

**Decision: Option A, adopted for v1.0.**

### 2.4 `denodo_columns` (Layer 2.5 — schema)

Not a layer in the tri-layer model itself — nested inside Layer 2 (describes *what columns a view has*, not the view or its resources).

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `view_name` | str | No | **Key (part 1).** |
| `db_name` | str | No | **Key (part 2).** |
| `column_name` | str | No | **Key (part 3).** |
| `ordinal_position` | int | No | |
| `data_type` | str | No | |
| `description` | str | Yes | |

Sourced solely from `view-details` — Maxen has confirmed this endpoint is authoritative for column schema, so no cross-source reconciliation logic is required. **Open item:** this assumption has not been independently re-verified against a second source (e.g. VQL `DESC VIEW`); worth a quick spot-check, and worth confirming explicitly with Maxen (see Maxen question list).

### 2.5 `denodo_properties` (custom properties) — **Decision Point 2**

341 distinct properties were observed in Step 0; they cannot all be first-class columns.

**Option A — Wide.** 341 mostly-`None` columns on `denodo_views`.

**Option B — Long EAV (recommended).** `denodo_properties(view_name, db_name, property_name, property_value)`.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `view_name` | str | No | **Key (part 1).** |
| `db_name` | str | No | **Key (part 2).** |
| `property_name` | str | No | **Key (part 3).** |
| `property_value` | str | Yes | Values normalized per Section 3. |

*Rationale:* the data is sparse and the property set will drift over time without requiring schema migrations; DSI's `find()`/`find_cell()` search long tables naturally. Only a small number of high-coverage, high-value properties (e.g. `Data Classification`, `Business Unit`, `Update Frequency`) should be promoted to real columns on `denodo_views` if/when there's a concrete query need — not preemptively.

**Forward-drift evidence (PROD, 2026-07-28):** the drift assumption above is no longer hypothetical. PROD carries 43 property groups, of which **36 are DCAT-related** (W3C Data Catalog Vocabulary: Dataset, Distribution, LicenseDocument, ProvenanceStatement, AccessRestriction, …) — 33 on the GENERAL tab with zero assigned elements and 3 more with no populated values. This is the signature of a staged, not-yet-started standards-based metadata rollout. When it begins, the property set will grow substantially; the long EAV shape absorbs that with zero schema migrations, whereas the wide option would require one migration per new property. Rollout plan/timeline is Maxen question M6.

**Open item — CLOSED (dev 2026-07-27; PROD 2026-07-28):** the completeness question was first resolved on a sampled view on the `-b` test host (`test_additional_properties_tab_v10/v11.ipynb`): every populated non-Summary value was found in the `view-details` response, carried in `customTabPropertyMap` (sibling of `summaryPropertyMap` under `propertyInfo`). The remaining V1 generalization check was then executed on PROD (`test_additional_properties_tab_v12_PROD.ipynb`): rather than sampling, it enumerated **every assigned element of every non-Summary group on PROD** (4 assignments across all 43 groups; 1 populated value) and confirmed the populated value present in `view-details` with no gap — i.e. completeness holds on PROD **by exhaustion**. Verdict recorded verbatim in the run: *"PROD CONFIRMS the -b finding: all populated Additional-tab values are present in view-details … denodo_properties can rely on view-details alone, extracting the UNION of summaryPropertyMap + generalTabPropertyMap + customTabPropertyMap via the 'propertyName' key. Question CLOSED."* `generalTabPropertyMap` remains unobserved-populated, now with a known cause (no GENERAL-tab assignments exist anywhere on PROD — see rev 9 changelog b); its item schema is assumed identical to the shared 16-key schema until first observation.

---

## 3. Normalization Rules

Single authoritative section — all normalization logic in `denodo.py` should point back here rather than being re-derived ad hoc.

### 3.1 Dev/prod field mapping

| Dev field | Prod field | Canonical (DSI) name |
|---|---|---|
| `db` | `databaseName` | `db_name` |

The contract defines the canonical name; the client normalizes on ingest regardless of environment. This is the only field-name drift confirmed during Step 0 — no other dev/prod discrepancies were identified across the fields catalogued in Section 5. If a new drift is discovered later (e.g. when prod access begins), it is added to this table; nothing else in the contract changes.

### 3.2 HTML stripping

Order matters (Step 0 lesson — URLs live in `href` attributes, not text):

1. Extract URLs from `href` attributes first.
2. Strip HTML tags.
3. Unescape HTML entities.
4. Collapse whitespace.

### 3.3 URL classification

| URL pattern | Maps to |
|---|---|
| `etrm.live`, `docs.oracle.com` | `documentation_url` |
| `accessit.lanl.gov` | `access_instructions` |
| anything else | Logged for manual review — **never silently discarded or binned**. |

### 3.4 Null policy

`""`, `"NOLINK"`, and whitespace-only strings all normalize to `None`.

---

## 4. Edge-Case Policy

### 4.1 Fetch failures (`fetch_status='error'`)

Any view whose `view-details` call fails (non-200) is included as a row in `denodo_views` with `fetch_status='error'` and all other fields `None`, rather than silently dropped. The ingested row count always equals the list-endpoint count; failures are queryable, not hidden.

*Empirical history:* the policy's original anchor was the pre-migration 12-view `vlanl*task` cohort (consistent HTTP 500 across all of Step 0). **Rev 10:** post-migration dev carries a different, prod-origin set of 30 `vlanl*task` views, all returning 200 — the cohort no longer reproduces. The policy is retained as defensive design (fetch failures remain possible from transient errors, permissions, or future content), and the `error_500` golden fixture is synthetic until a live failure is observed again (rev 10 changelog b).

### 4.2 Views missing `propertyInfo.summaryPropertyMap`

Valid row in `denodo_views`; zero corresponding rows in `denodo_properties`. Not an error condition.

### 4.3 Duplicate view name across databases

Legal under Denodo's per-VDB namespacing, though not yet observed in practice — the Step 0 probe covered a single VDB, so no cross-database duplicate has been seen to date (rev 9 editorial correction). If/when one appears: two rows, distinguished by the composite key. The validator (Section 6, Work item E — separate deliverable) must assert key uniqueness explicitly rather than assuming it.

---

## 5. Field-Mapping Appendix

Every field this contract touches, where it comes from, how it's normalized, and where it lands. Fields the Step 0 probe deliberately ignored are listed too, with the reason, so a future reader doesn't rediscover the same dead end.

### 5.1 List endpoint (`GET /public/api/views`, `serverId` param)

Verified response shape: `{id, name, value, description, db|databaseName, elementType, elementSubtype, path, lastModificationDate, fields, deleted}`.

| API field | DSI column | Table | Normalization | Notes |
|---|---|---|---|---|
| `name` | `view_name` | all | none | |
| `db` (dev) / `databaseName` (prod) | `db_name` | all | dev/prod field-name mapping (Section 3.1) | |
| `description` | — | — | — | List-endpoint description is a duplicate/truncated form of the richer `view-details` description; the latter is authoritative (Section 5.2). Not double-ingested. |
| `elementType` | — | — | — | Used only as a filter (keep `"view"`, skip other element types); not persisted as a column. |
| `deleted` | — | — | — | Used only as a filter (skip if `true`); not persisted. |
| `id` | — | — | — | Internal Denodo element ID. Not exposed in v1.0 — no DSI query need identified yet. Flag as open item if Phase 2 write-back needs it for lookups. |
| `elementSubtype` | — | — | — | Out of scope for v1.0. Seen values include `"interface"`; no schema decision has been made on whether interface views need different handling. Open item. |
| `path` | — | — | — | Out of scope. Denodo folder/category path — could feed a future `tags`/`category` concept but no consumer identified yet. |
| `lastModificationDate` | — | — | — | Out of scope for v1.0. Candidate for a future `last_modified_at` column if incremental refresh becomes a requirement; not needed for a full-snapshot Phase 1 backend. |
| `fields` | — | — | — | Superseded by the fuller column list from `view-details` (Section 5.2); not used. |
| `value` | — | — | — | Not observed to carry meaningful content distinct from `name`/`description` during Step 0; not mapped. |

### 5.2 `view-details` endpoint (`viewName`, `databaseName`, `serverId` params)

#### Top-level fields

| API field | DSI column | Table | Normalization | Notes |
|---|---|---|---|---|
| `description` (RICH_TEXT) | `description` | `denodo_views` | HTML strip (Section 3.2) | |
| `description` (URL extracted from `href`) | `documentation_url` | `denodo_views` | HTML strip, URL classification (Section 3.3) | Scalar, not a list — justified by the 544-URLs/544-views 1:1 finding. |
| column list (name, type, position, description per entry) | `column_name`, `data_type`, `ordinal_position`, `description` | `denodo_columns` | none | Sole source; Maxen confirmed authoritative (Section 2.4 open item re: independent re-verification). |
| `connectionUris` (JDBC / ODBC 32- & 64-bit / REST / OData / GraphQL) | — | — | — | **Out of scope for v1.0, status upgraded (2026-07-27):** no longer "possibly boilerplate" — confirmed to be a real, per-view API resource. A whole-response scan of a saved `view-details` payload found all 6 URLs under top-level `connectionUris`, with view-specific paths (REST/OData URLs embed `db/view` names). However, every hostname is `localhost` (VDP ports 9999/9996/9443), so the URLs are not consumable as returned. Unblock condition = Maxen question M1 (where is the hostname configured; can it return a routable FQDN?). If M1 resolves, ingest lands in a reopened `denodo_resources` (Section 2.3 evidence update). Power BI/Tableau (`.pbids`/`.tds`) files did **not** appear in the response — presumed UI-generated from these URLs. |

#### `propertyInfo` — three property maps: `summaryPropertyMap`, `generalTabPropertyMap`, `customTabPropertyMap`

Shape: `{group_name: [{propertyName, visualValue, ...}, ...]}`.

> **Key correction (2026-07-27):** the item key is **`propertyName`**, not `name`. Earlier probe code (`extract_property_names`) read `prop.get("name")` and silently returned `{None}` on every call. Any code inheriting that pattern — including Step 0's `iter_properties()` and any earlier notebook reading `summaryPropertyMap` items — **must be audited and fixed** before reuse in `denodo.py`.

**Item schema (confirmed empirically):** `summaryPropertyMap` and `customTabPropertyMap` items share one identical 16-key schema: `propertyName`, `visualValue`, `visualValueToEdit`, `searchValue`, plus embedded group metadata (`groupId`, `groupName`, `placeToShowGroup`) and property-definition metadata (`propertyType`, `propertyPossibleValues`, `propertyDescription`, …). Two consequences for `denodo.py`: (1) one generic extractor iterates all three maps with a single code path; (2) **no separate `group-details` call is needed** — `placeToShowGroup` is embedded in every item. (`generalTabPropertyMap` was empty on the sampled view; its item schema is assumed identical but unobserved.)

**Property naming convention:** observed property names in the probe data are group-qualified — e.g. `Details/Access Role Request`, `ODS Details/ODS DB Link`, `AI Portal/A.I. Summary` — not bare names. The contract adopts the group-qualified form as the canonical `property_name` value in `denodo_properties` (it disambiguates same-named properties across groups for free). **Construction rule:** `property_name = item["groupName"] + "/" + item["propertyName"]` — both components come directly from the item's own fields; do not parse them out of any display string. The shorthand names used elsewhere in this document (`ODS DB Link`, `Access Role Request`) refer to these qualified forms.

**Coexistence evidence (probe, 2026-07-22):** both `Details/Access Role Request` (558 hits) and a distinct `Details/Access Request` (3 hits) exist in the data. This empirically confirms that the custom access-role property and what appears to be a separate access-request mechanism coexist — they must not be merged during normalization; each keeps its own qualified `property_name`.

| Property name (group) | DSI column | Table | Normalization | Notes |
|---|---|---|---|---|
| `ODS DB Link` | `source_system` | `denodo_views` | HTML strip; `"NOLINK"` → `None` (Section 3.4) | Reclassified from an apparent Layer-3 signal to Layer-1 lineage. 63% carry `EBS_LINK`-style Oracle db-link identifiers; 0/1,347 sampled contained a URL. |
| `Access Role Request` | `access_instructions` | `denodo_views` | HTML strip; URL extraction | 93% are `accessit.lanl.gov` URLs — LANL's access portal, not documentation. |
| `Data Classification` | `property_value` (`property_name="Data Classification"`) | `denodo_properties` | HTML strip | Not promoted to its own column for v1.0 — candidate for future promotion if a concrete query need arises (Section 2.5). |
| `Business Unit` | `property_value` (`property_name="Business Unit"`) | `denodo_properties` | HTML strip | Same as above. |
| `Update Frequency` | `property_value` (`property_name="Update Frequency"`) | `denodo_properties` | HTML strip | Same as above. |
| `Primary POC`, `Secondary POC`, `Data Owner` | `property_value` (respective `property_name`) | `denodo_properties` | HTML strip; URL classified as `person_link`, **not** reclassified into `documentation_url`/`access_instructions` | These carry `pbplus.lanl.gov` (LANL people-directory) links — contacts, not documents or access instructions. Stored as ordinary property values, since no dedicated `contacts` table is in this version of the contract. |
| *(all other property names — up to 341 distinct)* | `property_value` (matching `property_name`) | `denodo_properties` | HTML strip; generic URL extraction where present | Default handling for any property not explicitly reclassified above. |

**Completeness — CLOSED (dev 2026-07-27; PROD 2026-07-28):** the completeness question for the three maps is resolved — see Section 2.5. The dev/`-b` test found non-Summary content carried in `customTabPropertyMap`; the PROD re-run (`v12_PROD`) then verified completeness **by exhaustion** over every non-Summary assignment on PROD. The `placeToShow` enum is now fully observed (`SUMMARY_TAB` / `SPECIFIC_CUSTOM_PROPERTY_TAB` / `GENERAL_CUSTOM_PROPERTY_TAB`) and corresponds 1:1 to the three maps; `generalTabPropertyMap` is empty on both environments because no GENERAL-tab group has any assigned element on PROD.

### 5.3 Derived / computed fields (not sourced from any API field)

| DSI column | Table | Derivation |
|---|---|---|
| `fetch_status` | `denodo_views` | `"ok"` if `view-details` returns HTTP 200; `"error"` if HTTP 500 (Section 4.1). Not a Denodo field. |
| `fetched_at` | all | Client-side timestamp at fetch time, ISO-8601. |
| `source_env` | all | Client-side context (`"dev"`/`"prod"`), not returned by the API. |
| `view_count` | `denodo_databases` | Computed by counting ingested `denodo_views` rows per `db_name`. |
| `server_id` | `denodo_databases` | The `serverId` *request parameter* the client sends (currently `1`; confirmed unchanged on PROD post-migration, 2026-07-28) — client context echoed into the row, not returned by the API. *(Added in rev 9 — previously unmapped.)* |

### 5.4 Database-level fields *(added in rev 9 — previously unmapped)*

| DSI column | Table | Intended source | Status |
|---|---|---|---|
| `db_name` | `denodo_databases` | Distinct `db`/`databaseName` values from the list endpoint (Section 5.1). | Verified. |
| `description` | `denodo_databases` | `GET /public/api/browse/databases` (database-level metadata endpoint from the endpoint-classification shortlist). | **Open item:** response shape not yet verified against this contract; column is nullable, so v1.0 may ingest `None` until verified. One-call check; fold result into the next changelog. |

---

## 6. Remaining Work (not covered by this draft)

- **Work item E** — Golden fixtures, now **8**: 5 query-selected from `probe_results.db` still valid on post-migration dev (`doc_url`, `access_role`, `ods_link`, `resource_rich`, plus re-selected `plain` = `inventory_daily_balance_fact`), 1 hand-picked `custom_tab`, 1 **synthetic** `error_500` (rev 10 b), and 1 new `second_vdb` (`ops_core_publication.i_ods_pa_cpnt_evthst`, rev 10 d). Fetch of the amended fixtures + validator run are the remaining steps.
- **Work item F** — Divya sign-off message presenting Decision Points 1 and 2, plus the rev 8 evidence updates, the rev 9 PROD confirmations, and the rev 10 environment note (dev re-seed, second VDB, fixture amendments).
- **Changelog** — revs 8–10 recorded in the header. Next expected entries: amended-fixture fetch results + validator pass, `denodo_databases.description` source verification (Section 5.4), Maxen answers M1–M7, first observation of a populated `generalTabPropertyMap` item (expected only after the DCAT rollout begins), first live fetch failure re-anchoring 4.1.
