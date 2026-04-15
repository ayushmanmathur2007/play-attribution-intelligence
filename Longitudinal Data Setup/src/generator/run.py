"""CLI entrypoint for the generator.

Usage (from inside `Longitudinal Data Setup/`):
    python -m src.generator.run
"""

from .events import main

if __name__ == "__main__":
    main()
