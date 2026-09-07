# StreamForge Engineering & Learning Notes

## Sprint 1 Takeaways

- **OpenSpec Workflow**: Combining Spec-Driven Development with modern AI-native tools streamlines architectural alignment through explicit phases (`proposal` -> `design` -> `specs` -> `tasks` -> `archive`).
- **Modern Python Toolchain (`uv` + `ruff` + `mypy`)**:
  - `uv` provides unified virtualenv and dependency locking in milliseconds.
  - Strict type checking (`disallow_untyped_defs = true`, `explicit_package_bases = true`) ensures high reliability across modular microservices.
- **Microservice Layout**:
  - Clear separation between independent services (`src/services/*`) and shared primitives (`src/shared/*`).
  - Separation between persistent architectural documentation (`docs/`) and living change specifications (`openspec/`).
