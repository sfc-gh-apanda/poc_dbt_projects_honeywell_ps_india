# Future Implementation Roadmap

## Overview

This document outlines the remaining branches to implement after the **smallest isolated branch (AR Aging)** is complete.

## Implemented ✅

### Branch 1: AR Aging (COMPLETE)
```
Source (FACT_AR) → Foundation (stg_ar_invoice, dim_customer, dim_fiscal_calendar) → Finance (dm_fin_ar_aging_simple)
```
- **Status**: Implemented
- **Models**: 4 total (1 staging, 2 dimensions, 1 mart)
- **Build Time**: ~20 seconds
- **Use Case**: Accounts receivable aging analysis

---

## To Be Implemented 📋

### Branch 2: AR Invoice (Next Priority)

```
Source (FACT_AR) → Foundation (stg_ar_invoice, dim_customer, dim_entity) → Finance (dm_fin_ar_invoice_external)
```

**New Components Needed**:
- Foundation: `dim_entity.sql` (legal entity dimension)
- Finance: `dm_fin_ar_invoice_external.sql` (invoice details mart)

**Estimated Effort**: 1 day  
**Build Time**: ~25 seconds

**Models to Create**:
```sql
-- dbt_foundation/models/marts/shared/dim_entity.sql
-- dbt_finance_core/models/marts/finance/dm_fin_ar_invoice_external.sql
```

---

### Branch 3: GL Balance Sheet

```
Source (FACT_GL_BS) → Foundation (stg_gl_balancesheet, dim_gl_account) → Finance (dm_fin_gl_balancesheet)
```

**New Components Needed**:
- Foundation: 
  - `stg_gl_balancesheet.sql` (GL BS staging)
  - `dim_gl_account.sql` (GL account dimension)
- Finance:
  - `dm_fin_gl_balancesheet.sql` (balance sheet mart)

**Estimated Effort**: 2 days  
**Build Time**: ~40 seconds (larger volume)

**Additional Complexity**:
- Questionnaire logic (Q-codes)
- Clearing account calculations
- Multiple source system consolidation

---

### Branch 4: GL Trial Balance

```
Source (FACT_GL_TB) → Foundation (stg_gl_trialbal) → Finance (dm_fin_gl_trialbal)
```

**New Components Needed**:
- Foundation: `stg_gl_trialbal.sql`
- Finance: `dm_fin_gl_trialbal.sql`

**Estimated Effort**: 1 day  
**Build Time**: ~35 seconds

---

### Branch 5: Revenue Margin (New Domain Project!)

```
Source (FACT_AR) → Foundation (existing) → Revenue (dm_rev_margin_detail)
```

**New Components Needed**:
- **New Project**: `dbt_revenue/`
- Revenue: `dm_rev_margin_detail.sql`

**Estimated Effort**: 2 days  
**Build Time**: ~30 seconds

**Note**: This demonstrates creating a second domain project with zero dependency on finance_core

---

### Branch 6: Projects GPMO (New Domain Project!)

```
Source (FACT_PROJECTS) → Foundation (stg_projects, dim_project) → Projects (dm_fin_gpmo_projects)
```

**New Components Needed**:
- **New Project**: `dbt_projects/`
- Foundation:
  - `stg_projects.sql`
  - `dim_project.sql`
- Projects: `dm_fin_gpmo_projects.sql`

**Estimated Effort**: 3 days  
**Build Time**: ~25 seconds

---

### Branch 7: Intercompany Reconciliation (New Domain Project!)

```
Source (FACT_AR, FACT_AP) → Foundation (existing) → Interco (dm_fin_interco_arap)
```

**New Components Needed**:
- **New Project**: `dbt_interco/`
- Foundation: `stg_ap_invoice.sql` (AP staging)
- Interco: `dm_fin_interco_arap.sql`

**Estimated Effort**: 2 days  
**Build Time**: ~20 seconds

---

## Implementation Priority Matrix

| Branch | Priority | Effort | Business Value | Dependencies |
|--------|----------|--------|----------------|--------------|
| AR Aging | ✅ Done | - | High | None |
| AR Invoice | 1 | 1 day | High | AR Aging complete |
| GL Balance Sheet | 2 | 2 days | High | None |
| GL Trial Balance | 3 | 1 day | Medium | GL BS complete |
| Revenue Margin | 4 | 2 days | High | None (new project) |
| Projects GPMO | 5 | 3 days | Medium | None (new project) |
| Interco Recon | 6 | 2 days | Medium | AR Invoice, AP staging |

---

## Detailed Implementation Plans

### Branch 2: AR Invoice (Detailed)

#### Step 1: Add dim_entity to Foundation (2 hours)

```sql
-- dbt_foundation/models/marts/shared/dim_entity.sql
{{
    config(
        materialized='table',
        tags=['shared', 'dimension', 'entity']
    )
}}

with source as (
    select * from {{ source('corp_master', 'dim_entity') }}
),

transformed as (
    select
        source_entity_code_sk || '|' || source_system as entity_id,
        source_entity_code_sk as company_code,
        source_system,
        entity_name,
        entity_country_name,
        entity_global_region,
        entity_status,
        current_timestamp() as _loaded_at
    from source
)

select * from transformed
```

#### Step 2: Add schema contract (1 hour)

```yaml
# dbt_foundation/models/marts/shared/_shared.yml
models:
  - name: dim_entity
    access: public
    config:
      contract:
        enforced: true
    columns:
      - name: entity_id
        data_type: varchar
        constraints:
          - type: not_null
          - type: primary_key
      # ... etc
```

#### Step 3: Create AR Invoice Mart (3 hours)

```sql
-- dbt_finance_core/models/marts/finance/dm_fin_ar_invoice_external.sql
-- Similar to AR aging but with invoice-level detail
-- Add billing type, distribution channel, etc.
```

#### Step 4: Add Tests (1 hour)

```yaml
# dbt_finance_core/models/marts/finance/_finance.yml
models:
  - name: dm_fin_ar_invoice_external
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - source_system
            - document_number
            - document_line
```

#### Step 5: Run & Validate (1 hour)

```bash
cd dbt_foundation && dbt run --select dim_entity
cd dbt_finance_core && dbt run --select dm_fin_ar_invoice_external
dbt test --select dm_fin_ar_invoice_external
```

---

### Branch 3: GL Balance Sheet (Detailed)

#### Components Needed

**Foundation Models**:
1. `stg_gl_balancesheet.sql` - Staging model for GL BS fact
2. `dim_gl_account.sql` - GL account dimension with mappings
3. `macro: questionnaire_logic.sql` - Q-code assignment logic

**Finance Models**:
1. `int_gl_balancesheet_cte.sql` - Intermediate model with CTEs
2. `dm_fin_gl_balancesheet.sql` - Final data mart

**Estimated Lines of Code**: ~800 lines (complex transformations)

---

### Branch 5: Revenue Margin (New Project - Detailed)

#### Create New Project Structure

```bash
mkdir dbt_revenue
cd dbt_revenue
```

```
dbt_revenue/
├── dbt_project.yml
├── dependencies.yml              # Only dbt_foundation
├── models/
│   └── marts/
│       └── revenue/
│           ├── _revenue.yml
│           └── dm_rev_margin_detail.sql
```

#### Configuration

```yaml
# dbt_revenue/dbt_project.yml
name: 'dbt_revenue'
version: '1.0.0'

models:
  dbt_revenue:
    marts:
      revenue:
        +materialized: table
        +schema: dm_rev
```

```yaml
# dbt_revenue/dependencies.yml
projects:
  - name: dbt_foundation
    version: ">=1.0.0,<2.0.0"
```

**Key Point**: Revenue project has ZERO dependency on finance_core! Only on foundation.

---

## Macro Library Expansion

As you implement more branches, expand the foundation macro library:

### Current Macros ✅
- `aging_bucket()` - Aging bucket calculation
- `fiscal_period()` - Fiscal period lookup

### Future Macros 📋

1. **customer_type_flag()**
```sql
{% macro customer_type_flag(customer_name, customer_type, source_system) %}
-- Complex customer type classification logic
{% endmacro %}
```

2. **currency_convert()**
```sql
{% macro currency_convert(amount, from_currency, to_currency, date) %}
-- Currency conversion with exchange rate lookup
{% endmacro %}
```

3. **questionnaire_logic()**
```sql
{% macro questionnaire_logic() %}
-- GL balance sheet questionnaire assignment (Q-codes)
{% endmacro %}
```

4. **fiscal_period_days()**
```sql
{% macro fiscal_period_days(fiscal_period) %}
-- Number of days in a fiscal period
{% endmacro %}
```

---

## Testing Strategy Evolution

### Current Tests ✅
- Source freshness
- Not null constraints
- Unique combinations
- Accepted values

### Future Tests 📋

1. **Reconciliation Tests**
```sql
-- tests/generic/test_amounts_reconcile.sql
-- Verify amounts sum correctly across layers
```

2. **Referential Integrity**
```sql
-- tests/generic/test_foreign_key.sql
-- Verify all customer_ids exist in dim_customer
```

3. **Business Rule Tests**
```sql
-- tests/generic/test_gl_balance_rule.sql
-- Verify debits = credits
```

4. **Data Freshness**
```sql
-- tests/generic/test_data_current.sql
-- Verify data is not older than X days
```

---

## Performance Optimization Path

### Current (Smallest Branch)
- All views in staging
- All tables in marts
- No partitioning
- No incremental models

### Phase 2 (After 3-4 Branches)
- Add clustering to large tables
- Implement incremental for historical data
- Add partitioning by fiscal period

### Phase 3 (Production Scale)
- Implement snapshots for SCD Type 2
- Add pre-aggregation tables
- Optimize query performance

---

## Deployment Strategy

### Current: Manual Deployment
```bash
dbt run --project dbt_foundation
dbt run --project dbt_finance_core
```

### Phase 2: Orchestrated Deployment
```python
# Airflow DAG
foundation_task >> [finance_task, revenue_task, projects_task]
```

### Phase 3: CI/CD Pipeline
```yaml
# GitHub Actions
on: push
  - run: dbt test --project dbt_foundation
  - run: dbt run --project dbt_foundation
  - run: dbt test --select state:modified
  - deploy: production
```

---

## Success Metrics by Branch

| Branch | Models | Tests | Build Time | Coverage |
|--------|--------|-------|------------|----------|
| AR Aging | 4 | 12 | 20s | ✅ 100% |
| AR Invoice | 5 | 15 | 25s | Target 100% |
| GL Balance Sheet | 7 | 20 | 40s | Target 100% |
| GL Trial Balance | 6 | 18 | 35s | Target 100% |
| Revenue Margin | 6 | 15 | 30s | Target 100% |
| Projects GPMO | 8 | 22 | 25s | Target 100% |
| Interco Recon | 6 | 16 | 20s | Target 100% |

---

## Key Principles for All Future Branches

1. ✅ **Foundation First**: All source data access goes through foundation
2. ✅ **Zero Lateral Dependencies**: Domain projects only reference foundation
3. ✅ **Schema Contracts**: All published models have enforced contracts
4. ✅ **Comprehensive Testing**: 100% test coverage on data marts
5. ✅ **Documentation**: Every model fully documented
6. ✅ **Isolation**: Each domain can deploy independently

---

## Next Steps

1. **Immediate** (Week 1):
   - Complete AR Invoice branch
   - Add dim_entity to foundation

2. **Short Term** (Weeks 2-4):
   - Implement GL Balance Sheet
   - Implement GL Trial Balance
   - Create integration tests

3. **Medium Term** (Weeks 5-8):
   - Create dbt_revenue project
   - Implement revenue margin models
   - Set up Airflow orchestration

4. **Long Term** (Weeks 9-16):
   - Create dbt_projects project
   - Create dbt_interco project
   - Implement CI/CD pipeline
   - Production deployment

---

**Remember**: Each branch follows the same pattern:
1. Add staging models to foundation (if new source)
2. Add shared dimensions to foundation (if needed)
3. Add macros to foundation (if reusable)
4. Create domain-specific marts in domain project
5. Add comprehensive tests
6. Document everything
7. Deploy independently

This ensures **true isolation** while maximizing **code reuse** through the foundation layer.

