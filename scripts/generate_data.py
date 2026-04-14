#!/usr/bin/env python3
"""CLI: Generate synthetic data for the Play Attribution system."""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_generator.generator import DataGenerator


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Play Store data")
    parser.add_argument("--output", default="data/synthetic/", help="Output directory")
    parser.add_argument("--months", type=int, default=18, help="Number of months of data")
    parser.add_argument("--full", action="store_true", help="Generate full dataset (all segments)")
    args = parser.parse_args()

    print(f"Generating {args.months} months of synthetic data...")
    print(f"Output directory: {args.output}")
    print(f"Mode: {'full' if args.full else 'sampled (3 key segments)'}")
    print()

    generator = DataGenerator(
        output_dir=args.output,
        months=args.months,
    )
    generator.generate()

    print("\nData generation complete!")
    print(f"Files written to: {args.output}")


if __name__ == "__main__":
    main()
