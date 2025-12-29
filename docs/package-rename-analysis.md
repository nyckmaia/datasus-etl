# Package Rename Analysis - Melhoria 09

## Overview

Analysis of renaming the package from `pydatasus` to `datasus` on PyPI.

## Current State

- **Package name**: `pydatasus`
- **PyPI URL**: https://pypi.org/project/pydatasus/
- **Install**: `pip install pydatasus`
- **Import**: `from pydatasus import ...`

## Target State

- **Package name**: `datasus`
- **PyPI URL**: https://pypi.org/project/datasus/
- **Install**: `pip install datasus`
- **Import**: `from datasus import ...`

## PyPI Name Availability

### Check Method

```bash
pip index versions datasus
```

Or visit: https://pypi.org/project/datasus/

### Possible Outcomes

1. **Available**: Name is free to use
2. **Taken**: Another project owns the name
3. **Reserved**: PyPI has reserved the name (unlikely)

### If Name is Taken

Options:
- Contact current owner about transfer
- Use alternative names: `datasus-etl`, `datasus-pipeline`, `py-datasus`
- Keep current name `pydatasus`

## Migration Strategy

### Option A: Clean Break

1. Publish new package `datasus`
2. Deprecate `pydatasus` (keep on PyPI with warning)
3. Users migrate to new package

**Pros**:
- Clean, simple
- No compatibility shims

**Cons**:
- Breaking change for existing users
- Need to maintain deprecation notice

### Option B: Dual Publishing

1. Publish both `datasus` and `pydatasus`
2. `pydatasus` becomes a thin wrapper

```python
# pydatasus/__init__.py (wrapper)
import warnings
warnings.warn(
    "pydatasus is deprecated, use 'datasus' instead",
    DeprecationWarning
)
from datasus import *
```

**Pros**:
- Backwards compatible
- Gradual migration

**Cons**:
- Maintain two packages
- Confusing for new users

### Option C: Alias Package

1. Main package: `datasus`
2. Install alias: `pip install pydatasus` installs `datasus`

```toml
# pyproject.toml for pydatasus (alias)
[project]
name = "pydatasus"
version = "2.0.0"
dependencies = ["datasus>=2.0.0"]
```

**Pros**:
- Single codebase
- Automatic migration via dependency

**Cons**:
- Complex setup
- Two PyPI entries to maintain

## Code Changes Required

### 1. Directory Rename

```
src/pydatasus/ → src/datasus/
```

### 2. Import Updates (all files)

```python
# Before
from pydatasus.config import PipelineConfig
from pydatasus.pipeline import SihsusPipeline

# After
from datasus.config import PipelineConfig
from datasus.pipeline import SihsusPipeline
```

### 3. pyproject.toml

```toml
[project]
name = "datasus"  # Changed
# ...

[project.scripts]
datasus = "datasus.cli:app"  # Changed
```

### 4. CLI Entry Point

Current:
```bash
datasus run --subsystem sihsus
```

No change needed (already using `datasus` command).

### 5. Documentation

- Update all README references
- Update examples
- Update installation instructions

## Files to Modify

| File | Changes |
|------|---------|
| `pyproject.toml` | name, scripts, packages |
| `src/pydatasus/` → `src/datasus/` | Rename directory |
| All `*.py` files | Update imports |
| `README.md` | Update package name |
| `examples/*.py` | Update imports |
| `tests/*.py` | Update imports |
| `.github/workflows/*.yml` | Update if present |

## Estimated Impact

### Grep for Import Count

```bash
grep -r "from pydatasus" --include="*.py" | wc -l
grep -r "import pydatasus" --include="*.py" | wc -l
```

### Automated Migration Script

```python
import re
from pathlib import Path

def migrate_imports(root: Path):
    for py_file in root.rglob("*.py"):
        content = py_file.read_text()
        new_content = content.replace("pydatasus", "datasus")
        if new_content != content:
            py_file.write_text(new_content)
            print(f"Updated: {py_file}")
```

## Recommendation

### If `datasus` is available on PyPI:

1. **Do the rename** - shorter, cleaner name
2. Use **Option A (Clean Break)** for simplicity
3. Keep `pydatasus` on PyPI with deprecation warning
4. Bump major version (1.x → 2.0.0) to signal breaking change

### If `datasus` is taken:

1. **Keep `pydatasus`** - it's already established
2. Consider alternatives only if current owner is inactive
3. Focus on other improvements instead

## Timeline Considerations

- Best done during major version bump
- Coordinate with documentation updates
- Announce deprecation in advance (1-2 releases)
- Provide migration guide for users

## Risks

1. **User confusion**: Two package names in wild
2. **Search/SEO impact**: PyPI search ranking reset
3. **CI/CD breakage**: Users' automated pipelines fail
4. **Documentation rot**: Old tutorials reference old name

## Mitigation

1. Clear deprecation warnings in old package
2. Redirect/link from old PyPI page (via description)
3. Major version bump signals breaking change
4. Comprehensive migration documentation
