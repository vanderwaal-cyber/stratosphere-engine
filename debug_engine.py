import sys
import logging

# Configure basic logging to stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("debug_engine")

print("--- 1. Testing Imports ---")
try:
    import bs4
    print("✅ bs4 imported successfully")
except ImportError as e:
    print(f"❌ Failed to import bs4: {e}")

try:
    from collectors.github import GithubCollector
    print("✅ GithubCollector imported")
except Exception as e:
    print(f"❌ Failed to import GithubCollector: {e}")

try:
    from collectors.launchpads import LaunchpadCollector
    print("✅ LaunchpadCollector imported")
except Exception as e:
    print(f"❌ Failed to import LaunchpadCollector: {e}")

try:
    from core.engine import StratosphereEngine
    print("✅ StratosphereEngine imported")
except Exception as e:
    print(f"❌ Failed to import StratosphereEngine: {e}")
    # Print traceback
    import traceback
    traceback.print_exc()

print("\n--- 2. Testing Engine Instantiation ---")
try:
    engine = StratosphereEngine()
    print("✅ Engine instantiated")
except Exception as e:
    print(f"❌ Engine instantiation failed: {e}")
