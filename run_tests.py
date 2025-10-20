#!/usr/bin/env python3
"""Test runner script for F1 Pipeline tests."""

import sys
import subprocess
import argparse
from pathlib import Path


def run_tests(test_type="all", coverage=True, verbose=False):
    """Run tests with specified parameters."""
    
    cmd = ["python", "-m", "pytest"]
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    
    # Add coverage if requested
    if coverage:
        cmd.extend([
            "--cov=f1_pipeline",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov"
        ])
    
    # Select test type
    if test_type == "unit":
        cmd.extend(["-m", "unit"])
    elif test_type == "integration":
        cmd.extend(["-m", "integration"])
    elif test_type == "fast":
        cmd.extend(["-m", "not slow"])
    elif test_type == "slow":
        cmd.extend(["-m", "slow"])
    elif test_type == "aws":
        cmd.extend(["-m", "aws"])
    elif test_type == "azure":
        cmd.extend(["-m", "azure"])
    elif test_type == "gcp":
        cmd.extend(["-m", "gcp"])
    elif test_type == "local":
        cmd.extend(["-m", "not (aws or azure or gcp)"])
    # "all" runs everything
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    return result.returncode


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run F1 Pipeline tests")
    parser.add_argument(
        "--type", 
        choices=["all", "unit", "integration", "fast", "slow", "aws", "azure", "gcp", "local"],
        default="all",
        help="Type of tests to run"
    )
    parser.add_argument(
        "--no-coverage",
        action="store_true",
        help="Disable coverage reporting"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true", 
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    return_code = run_tests(
        test_type=args.type,
        coverage=not args.no_coverage,
        verbose=args.verbose
    )
    
    sys.exit(return_code)


if __name__ == "__main__":
    main()