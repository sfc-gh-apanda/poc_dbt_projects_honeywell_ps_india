# ⚠️ THIS PROJECT IS DEPRECATED

**Date:** December 2, 2025  
**Status:** ❌ DEPRECATED - DO NOT USE

---

## 🚫 Why This Project Is No Longer Used

**Snowflake Native dbt projects do not support cross-project references.**

This `dbt_o2c_semantic` project attempted to reference models from `dbt_o2c`, which works in dbt Cloud/CLI but **fails in Snowflake Native Apps**.

---

## ✅ What to Use Instead

**All functionality has been consolidated into the `dbt_o2c` project.**

### **Use This:**
```bash
cd O2C/dbt_o2c       ← Build from here!
dbt deps
dbt build
```

### **NOT This:**
```bash
cd O2C/dbt_o2c_semantic    ← DO NOT use this anymore!
```

---

## 📊 New Architecture

```
dbt_o2c/                    ← Single unified project
├── models/
│   ├── staging/            (3 models)
│   ├── marts/              (5 models)
│   └── semantic_views/     ← Moved here from dbt_o2c_semantic
│       ├── sv_o2c_reconciliation.sql
│       └── sv_o2c_customer_summary.sql
```

---

## 🗑️ Can This Folder Be Deleted?

**Yes!** This entire `dbt_o2c_semantic` folder can be safely deleted.

All semantic view functionality is now in:
- `O2C/dbt_o2c/models/semantic_views/`

---

## 📚 Documentation

For current documentation, see:
- `O2C/O2C_README.md` (updated architecture)
- `O2C/dbt_o2c/README.md` (includes semantic views)
- `O2C/O2C_DATA_FLOW_LINEAGE.md` (updated data flow)

---

**For any questions, refer to the consolidated `dbt_o2c` project.**

