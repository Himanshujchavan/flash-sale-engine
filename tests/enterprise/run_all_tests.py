#!/usr/bin/env python3
"""
Runner script for all enterprise-level tests.
Executes all test modules in the enterprise test suite.
"""
import subprocess
import sys
import time
import os

def run_test_module(module_name):
    """Run a single test module and return results."""
    print(f"\n{'='*60}")
    print(f"Running {module_name}")
    print(f"{'='*60}")

    start_time = time.time()

    try:
        # Run the test module using pytest
        result = subprocess.run([
            sys.executable, '-m', 'pytest',
            f'tests/enterprise/{module_name}.py',
            '-v',  # verbose output
            '--tb=short'  # shorter traceback format
        ], capture_output=True, text=True, timeout=120)

        end_time = time.time()
        duration = end_time - start_time

        print(f"Duration: {duration:.2f} seconds")
        print(f"Return code: {result.returncode}")

        if result.stdout:
            print("STDOUT:")
            print(result.stdout)

        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        return {
            'module': module_name,
            'success': result.returncode == 0,
            'duration': duration,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        }

    except subprocess.TimeoutExpired:
        print(f"TEST TIMEOUT: {module_name} exceeded 120 seconds")
        return {
            'module': module_name,
            'success': False,
            'duration': 120.0,
            'stdout': '',
            'stderr': 'Test timed out after 120 seconds',
            'returncode': -1
        }
    except Exception as e:
        print(f"ERROR running {module_name}: {e}")
        return {
            'module': module_name,
            'success': False,
            'duration': 0.0,
            'stdout': '',
            'stderr': str(e),
            'returncode': -2
        }

def main():
    """Run all enterprise test modules."""
    print("Starting Enterprise Test Suite for Flash Sale Engine")
    print("=" * 60)

    # List of test modules to run
    test_modules = [
        'test_concurrency_oversell',
        'test_idempotency',
        'test_saga_compensation',
        'test_transactional_outbox',
        'test_chaos_fault_injection',
        'test_ordering_delivery',
        'test_rate_limiting_backpressure',
        'test_reconciliation',
        'test_observability'
    ]

    results = []
    start_time = time.time()

    # Run each test module
    for module in test_modules:
        result = run_test_module(module)
        results.append(result)

        # Brief pause between tests
        time.sleep(1)

    end_time = time.time()
    total_duration = end_time - start_time

    # Summary
    print("\n" + "=" * 60)
    print("ENTERPRISE TEST SUITE SUMMARY")
    print("=" * 60)

    passed = sum(1 for r in results if r['success'])
    failed = len(results) - passed

    for result in results:
        status = "PASS" if result['success'] else "FAIL"
        print(f"{status:4} | {result['module']:35} | {result['duration']:6.2f}s")

    print("-" * 60)
    print(f"TOTAL: {passed} passed, {failed} failed, {len(results)} total")
    print(f"Duration: {total_duration:.2f} seconds")

    # Detailed failure information
    if failed > 0:
        print("\nFAILED TESTS:")
        for result in results:
            if not result['success']:
                print(f"  - {result['module']}:")
                print(f"    Return code: {result['returncode']}")
                if result['stderr']:
                    # Show first few lines of error
                    error_lines = result['stderr'].split('\n')[:5]
                    for line in error_lines:
                        if line.strip():
                            print(f"    {line}")

    # Return appropriate exit code
    return 0 if failed == 0 else 1

if __name__ == '__main__':
    sys.exit(main())