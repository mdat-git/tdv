PRD-001 — Invoice Eligibility Ledger (IEL)
Aka: Billable Scope Ledger • Ready-to-Invoice Ledger

Status: In build (target: vendor self-service + audit-grade eligibility snapshots)

1) What problem this solves (business context)

Vendors invoice at the FLOC line level. A single invoice can contain thousands of FLOCs, and if even one or two lines are invalid (missing required deliverables, wrong package, already invoiced, etc.), the invoice may be rejected, forcing the vendor to cancel and resubmit. This creates:

avoidable vendor churn/friction

AP/ops overhead

delays in payment and reporting

disputes with no single “as-of” truth to reference

Historically, the “Ready to Invoice” logic was computed inside Power BI Power Query (M-code) by joining raw sources and calculating flags at refresh time. That creates a critical governance issue:

There is no stable, auditable “fact table” of what we told the vendor they could invoice as of a specific time, because the logic is recomputed live as upstream systems change.

IEL exists to publish an auditable, timestamped record of invoice eligibility so the organization can confidently tell vendors:

what is approved to invoice

what is blocked and why

what has already been invoiced/paid
…all with traceability back to source evidence and “as-of” timestamps.

2) What the product is (high-level)

IEL is a department data product that produces a daily (or scheduled) eligibility snapshot at the billable grain:

scope_package_id × floc_id (plus snapshot_id)

It is built as a medallion/lakehouse pipeline:

BRONZE captures raw receipts from each source system (immutable)

SILVER standardizes and aggregates to join-safe grains

GOLD publishes official eligibility snapshots and vendor-facing views/exports

Power BI becomes presentation-only, consuming Gold outputs rather than computing eligibility logic.

3) Who uses it (consumers)

Primary consumers:

Vendors (self-service readiness + blockers + invoiced/paid)

Inspections Ops / Program team (triage blockers, validate readiness)

Finance/AP (invoice validation support, dispute resolution)

Internal stakeholders tracking scoped vs delivered work

4) Core concepts and definitions
4.1 Scope package (award)

A scope package represents work officially awarded/permitted to a vendor. It defines the set of FLOCs the vendor is authorized to complete and invoice.

Key implication:

A FLOC can legitimately appear across multiple packages over time (reassignment).
IEL must support full history and also provide a current assignment view.

4.2 Evidence (deliverables)

Eligibility depends on evidence arriving in various systems, e.g.:

surveys from inspection apps (Azure now, Salesforce later)

image metadata from GCP

deliveries / completion signals (source may vary)

4.3 Invoice lines

Invoices are modeled at the FLOC line level and include scope_package_id.
This lets IEL prevent vendors from invoicing already invoiced lines and enables paid status tracking.

4.4 Snapshotting (audit)

IEL publishes a snapshot: a frozen “official record” of eligibility at time as_of_ts.
This is the core audit feature that removes ambiguity and prevents “Power BI recompute drift.”

5) Data contract: grain and keys

IEL is intentionally designed around explicit grains to prevent join mistakes.

Canonical grains used in IEL (department registry IDs)

GRN-001 upload_file — 1 row per upload (upload_id)

GRN-002 upload_row — 1 row per uploaded CSV row (upload_id, row_num)

GRN-003 scope_package_header — 1 row per scope package (scope_package_id)

GRN-004 scope_package_floc_line — 1 row per package × FLOC (scope_package_id, floc_id)

GRN-005 assignment_history_event — 1 row per assignment event (floc_id, effective_start_ts)

GRN-006 evidence_by_scope_package_floc — join-safe evidence at package × FLOC

GRN-007 invoice_line_fact — invoice line (invoice_id, scope_package_id, floc_id)

GRN-008 eligibility_snapshot_line — snapshot line (snapshot_id, scope_package_id, floc_id)

GRN-009 eligibility_snapshot_scope_package_summary — summary (snapshot_id, scope_package_id)

6) Architecture: medallion model and why it exists
BRONZE: raw receipts (immutable)

Purpose: preserve “what the systems said” for replay/audit.
Bronze is not for dashboards.

Typical Bronze features:

append-only

includes source lineage (source_extract_ts, ingest_ts, file path/name, hashes)

minimal transformation (type normalization only if necessary)

SILVER: conformed, join-safe truth

Purpose: provide standardized tables and evidence aggregates that are safe to join and reuse.

Most important IEL rule:

Evidence tables must be aggregated to package × FLOC so eligibility joins are deterministic.

GOLD: published truth (vendor-trustable)

Purpose: publish eligibility decisions as auditable snapshots and stable views for consumption.

Gold objects include:

append-only snapshot tables

“current snapshot” views

optionally vendor exports (SharePoint)

7) Canonical object catalog (what exists)

All objects live under INSPECTIONS_<ENV> with schemas BRONZE, SILVER, GOLD, UTIL.

7.1 BRONZE objects (raw)
Object	Type	Grain	Produced by
BRONZE.raw_scope_package_upload	table	GRN-001	ETL-001
BRONZE.raw_scope_package_row	table	GRN-002	ETL-001
BRONZE.raw_azure_survey	table	raw	ETL-002
BRONZE.raw_gcp_image_metadata	table	raw	ETL-003
BRONZE.raw_gcp_delivery	table	raw	ETL-004
BRONZE.raw_ap_invoice_line	table	raw	ETL-005
7.2 SILVER objects (conformed)
Object	Type	Grain	Produced by
SILVER.scope_package_header	table	GRN-003	ETL-001
SILVER.scope_package_line	table	GRN-004	ETL-001
SILVER.scope_assignment_history	table	GRN-005	ETL-001
SILVER.evidence_azure_survey_by_scope_package_floc	table	GRN-006	ETL-002
SILVER.evidence_gcp_images_by_scope_package_floc	table	GRN-006	ETL-003
SILVER.evidence_gcp_deliveries_by_scope_package_floc	table	GRN-006	ETL-004
SILVER.invoice_line_fact	table	GRN-007	ETL-005
7.3 GOLD objects (published)
Object	Type	Grain	Produced by
GOLD.scope_assignment_current	table/view	GRN-004	ETL-006
GOLD.invoice_eligibility_snapshot_line	table	GRN-008	ETL-006
GOLD.invoice_eligibility_snapshot_scope_package_summary	table	GRN-009	ETL-006
GOLD.vw_vendor_invoice_ledger_current	view	GRN-004 (latest snapshot)	ETL-006
GOLD.vw_vendor_approved_to_invoice	view	GRN-004	ETL-006
GOLD.vw_vendor_blocked_items	view	GRN-004	ETL-006
GOLD.vw_vendor_invoiced_paid_history	view	GRN-004	ETL-006
8) Pipeline catalog (ETL registry summary)

IEL is decomposed into independent ETLs to separate concerns and allow scaling.

ETL-001 — Scope Package Intake (Streamlit upload)

Goal: ingest awarded scope packages and build conformed scope/assignment tables.
Writes: Bronze upload + row; Silver header + lines + assignment history.

ETL-002 — Survey Evidence (Azure)

Goal: ingest survey deliverables and aggregate evidence to package × FLOC.
Writes: Bronze raw + Silver evidence aggregate.

ETL-003 — Image Evidence (GCP)

Goal: ingest image metadata and aggregate to package × FLOC.
Writes: Bronze raw + Silver evidence aggregate.

ETL-004 — Deliveries Evidence

Goal: ingest deliveries/completion signals and aggregate to package × FLOC.
Writes: Bronze raw + Silver evidence aggregate.

ETL-005 — Invoice Lines Fact

Goal: ingest invoice lines (invoice_id + package + floc) and standardize billing ledger.
Writes: Bronze raw + Silver invoice_line_fact.

ETL-006 — Publish Eligibility Snapshot (official)

Goal: compute and publish the official “as-of” eligibility record.
Writes: Gold snapshot tables + “current snapshot” views.

ETL-007 (Optional) — Vendor SharePoint Export

Goal: push curated vendor files daily from the latest snapshot.
Writes: SharePoint CURRENT overwrite + HISTORY append.

9) Lineage (what depends on what)

This is the “registry lineage” view in narrative form.

The spine

SILVER.scope_package_line is the left-most join spine. Everything attaches to it at package × FLOC.

Evidence attaches at the same grain

SILVER.evidence_azure_survey_by_scope_package_floc

SILVER.evidence_gcp_images_by_scope_package_floc

SILVER.evidence_gcp_deliveries_by_scope_package_floc

Billing status attaches via invoices

SILVER.invoice_line_fact contributes invoiced_flg / paid_flg signals (usually aggregated in ETL-006 to the spine grain).

The publisher produces the only “truth vendors should consume”

ETL-006 creates:

GOLD.invoice_eligibility_snapshot_line (append-only official record)

GOLD.vw_vendor_* views (latest snapshot interface)

Power BI and vendor exports consume Gold only

Power BI uses GOLD.vw_vendor_invoice_ledger_current

Vendor files export from Gold views/snapshots

10) Outputs: what vendors will see (and how they use it)

IEL produces three core vendor-facing datasets (per vendor, per snapshot):

Approved to Invoice
Lines that are eligible and not already invoiced:

ready_to_invoice_flg = true

invoiced_flg = false

Blocked / Outstanding
Not eligible yet, with explicit blockers (missing survey, missing images, etc.):

ready_to_invoice_flg = false

invoiced_flg = false

includes blocker_codes / blocker_reason

Invoiced / Paid history
Already invoiced, optionally split by paid:

invoiced_flg = true

paid_flg indicates payment completion (if available)

Each dataset is tied to:

snapshot_id

as_of_ts

rule_version

So when disputes happen, you can say:

“This was the official approved set as of 2026-02-01 06:00 PT (snapshot XYZ).”

11) Governance guarantees (what makes this defensible)

IEL is designed to ensure:

immutability of raw receipts (Bronze)

join-safe conformed evidence (Silver)

auditable published decisions (Gold snapshots)

no business logic in Power BI (presentation only)

rule versioning (eligibility logic changes are traceable)

12) Registry integration (how IEL fits department-wide infrastructure)

IEL is registered as:

PRD-001 in the department registry

ETLs are ETL-001…ETL-007

Objects are assigned OBJ-#####

Grains are GRN-###

Exports are EXP-###

Lineage is captured as EDG-##### edges

This enables:

impact analysis

onboarding

compliance/audit response

operational dashboards (freshness/health)

13) Build plan

PRD-001 Build Plan (in priority order)
Step 1 — Define the contract for the spine

Spine table: SILVER.scope_package_line (scope_package_id × floc_id)

Lock down:

required columns (minimum)

allowed nulls (basically none for IDs)

uniqueness rule: (scope_package_id, floc_id) must be unique per package version upload (history OK across packages)

Why first: everything downstream depends on this grain being stable.

Step 2 — Build ETL-001 (Scope Package Intake) for real

This is the one you said you want: Streamlit upload → clean minimal fields → load.

Outputs:

BRONZE.raw_scope_package_upload

BRONZE.raw_scope_package_row

SILVER.scope_package_header

SILVER.scope_package_line

SILVER.scope_assignment_history (effective dating)

Key build decisions:

upload_id strategy (UUID is fine)

file_hash dedupe to prevent double-ingest

normalizing vendor_id and scope_package_id formats

Success criteria: You can ingest 3 packages and see correct rows in scope_package_line, and you can end-date/insert assignment history when a FLOC moves packages.

Step 3 — Stub the Evidence tables (even before full Azure/GCP automation)

You can ship value quickly by doing “thin” ETLs first.

Create these Silver tables with the correct grain and placeholder logic:

SILVER.evidence_azure_survey_by_scope_package_floc

SILVER.evidence_gcp_images_by_scope_package_floc

SILVER.evidence_gcp_deliveries_by_scope_package_floc

Even if initially they’re populated from CSV extracts / manual exports, you’re still building the correct architecture.

Success criteria: Each evidence table has at most 1 row per (scope_package_id, floc_id).

Step 4 — Build ETL-006 early (Eligibility Snapshot Publisher)

This is your “big win”: removing business logic from Power BI.

Output:

GOLD.invoice_eligibility_snapshot_line (append-only)

GOLD.invoice_eligibility_snapshot_scope_package_summary

the GOLD.vw_vendor_* views

Start with a simple rule:

ready = survey_received AND images_received (deliveries optional at first)

invoiced/paid = false until ETL-005 is wired

Success criteria: You can publish a snapshot and Power BI can display it without any M-query business logic.

Step 5 — Add ETL-005 (Invoice Line Fact) + integrate invoiced/paid flags

Once invoice data is wired:

SILVER.invoice_line_fact

update ETL-006 logic to compute:

invoiced_flg

paid_flg

Success criteria: “Approved to invoice” excludes already-invoiced FLOC lines.

Step 6 — Optional ETL-007 export to Vendor SharePoint

When you’re ready to operationalize:

write the “Approved / Blocked / Invoiced” CSVs daily

CURRENT/ overwrite + HISTORY/ append

Success criteria: Vendors can self-serve without dashboard hacks.

What we should do next (most actionable)

To actually start building PRD-001, pick one of these two entry points:

Option A (best): Start with ETL-001 Streamlit intake

If you’re ready to code right now, we’ll define:

minimal required columns in the upload CSV

mapping/validation rules

exact table schemas for the 5 objects ETL-001 writes

Option B (fastest visible win): Start with ETL-006 snapshot publisher

If you already have Power BI logic today, we can:

translate that M-query logic into SQL/Python transformations

publish the first GOLD.invoice_eligibility_snapshot_line

then backfill upstream ETLs cleanly
