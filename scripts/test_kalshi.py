"""Quick test: verify Kalshi API credentials are working."""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.data.kalshi_client import KalshiClient


async def main():
    key_id = os.getenv("KALSHI_API_KEY_ID", "")
    pk_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")

    if not key_id:
        print("KALSHI_API_KEY_ID not set in .env")
        print()
        print("Setup steps:")
        print("  1. Generate RSA key pair:")
        print("     openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out kalshi_private.key")
        print("     openssl rsa -in kalshi_private.key -pubout -out kalshi_public.pub")
        print()
        print("  2. Go to https://demo.kalshi.com -> Account & Security -> API Keys")
        print("     Paste the contents of kalshi_public.pub and save the Key ID")
        print()
        print("  3. Edit .env file:")
        print('     KALSHI_API_KEY_ID=your-key-id-here')
        print('     KALSHI_PRIVATE_KEY_PATH=/Users/ZK/DemoCode/PolyArbitrage/kalshi_private.key')
        return

    if pk_path and not Path(pk_path).exists():
        print(f"Private key file not found: {pk_path}")
        return

    use_demo = "--prod" not in sys.argv
    env_label = "DEMO" if use_demo else "PRODUCTION"
    print(f"Testing Kalshi API ({env_label})...")
    print(f"  Key ID: {key_id[:8]}...{key_id[-4:]}")
    print(f"  Private key: {pk_path}")
    print()

    client = KalshiClient(api_key_id=key_id, private_key_path=pk_path, use_demo=use_demo)

    try:
        data = await client.get_markets(limit=5)
        markets = data.get("markets", [])
        print(f"Connected! Found {len(markets)} markets (showing first 5):")
        print()
        for m in markets:
            title = m.get("title", "?")
            yes_ask = m.get("yes_ask", 0)
            vol = m.get("volume", 0)
            print(f"  [{m.get('ticker', '?')}] {title[:60]}")
            print(f"    YES ask: {yes_ask}c  Volume: {vol}")
            print()
        print("Kalshi API is working correctly!")
    except Exception as e:
        print(f"Error: {e}")
        print()
        if "401" in str(e) or "403" in str(e):
            print("Authentication failed. Check that:")
            print("  - API Key ID matches what Kalshi shows")
            print("  - Private key file is the correct one paired with the public key you uploaded")
            print("  - You're using the right environment (demo vs production)")
        elif "429" in str(e):
            print("Rate limited. Wait a minute and try again.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
