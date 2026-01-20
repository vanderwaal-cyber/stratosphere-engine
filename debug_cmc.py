import asyncio
import httpx
from core.config import get_settings

async def test_cmc():
    settings = get_settings()
    print(f"API Key: {settings.CMC_API_KEY[:5]}...")
    
    async with httpx.AsyncClient() as client:
        # Get Info for Ethereum (ID 1027) which has socials
        print("Fetching info for Ethereum (1027)...")
        coin_id = "1027"
        info_resp = await client.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info",
            headers={"X-CMC_PRO_API_KEY": settings.CMC_API_KEY},
            params={"id": coin_id}
        )
        details = info_resp.json().get("data", {}).get(coin_id, {})
        print(f"Coin: {details.get('name')}")
        
        # Test Extraction Logic
        urls = details.get("urls", {})
        logo = details.get("logo")
        
        print("\n--- EXTRACTION RESULTS ---")
        print(f"Logo: {logo}")
        
        twitter = None
        if urls.get("twitter") and isinstance(urls["twitter"], list) and len(urls["twitter"]) > 0:
            twitter = urls["twitter"][0]
        print(f"Twitter: {twitter} (Raw: {urls.get('twitter')})")
        
        telegram = None
        if urls.get("chat") and isinstance(urls["chat"], list):
            for chat in urls["chat"]:
                if "t.me" in chat or "telegram" in chat:
                    telegram = chat
                    print(f"Telegram (Match): {telegram}")
                    break
        print(f"Telegram Final: {telegram}")
        
if __name__ == "__main__":
    asyncio.run(test_cmc())
