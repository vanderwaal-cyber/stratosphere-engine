import re
from typing import Dict, Optional
from bs4 import BeautifulSoup

class SocialExtractor:
    def extract_all(self, html: str) -> Dict[str, Optional[str]]:
        """
        Extracts Twitter, Discord, Telegram, Email from HTML.
        Includes OpenGraph and Meta tags.
        """
        socials = {
            "twitter": None,
            "discord": None,
            "telegram": None,
            "email": None
        }
        
        if not html:
            return socials
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # 1. Meta Tags (High Confidence)
            # <meta name="twitter:site" content="@handle">
            meta_tw = soup.find('meta', attrs={'name': 'twitter:site'}) or soup.find('meta', attrs={'name': 'twitter:creator'})
            if meta_tw and meta_tw.get('content'):
                content = meta_tw['content'].strip()
                if content.startswith('@'): content = content[1:]
                socials['twitter'] = content

            # 2. Regex Patterns (Link Scanning)
            patterns = {
                "twitter": r'href=[\'"](?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)[\'"]',
                "discord": r'href=[\'"](?:https?://)?(?:discord\.gg|discord\.com/invite)/([a-zA-Z0-9]+)[\'"]',
                "telegram": r'href=[\'"](?:https?://)?t\.me/([a-zA-Z0-9_]+)[\'"]',
                "email": r'href=[\'"]mailto:([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)[\'"]'
            }
            
            for key, pattern in patterns.items():
                if socials[key]: continue # Skip if found via meta
                
                matches = re.findall(pattern, html)
                # Filter Bad Matches
                valid = []
                for m in matches:
                    lower_m = m.lower()
                    if key == 'twitter' and lower_m in ['intent', 'share', 'home', 'explore', 'search', 'status', 'hashtags']:
                        continue
                    if key == 'telegram' and lower_m in ['share', 'contact', 'joinchat']:
                        continue
                    valid.append(m)
                    
                if valid:
                    # Basic formatting
                    if key == 'discord': socials[key] = f"https://discord.gg/{valid[0]}"
                    elif key == 'telegram': socials[key] = f"https://t.me/{valid[0]}"
                    elif key == 'email': socials[key] = valid[0]
                    else: socials[key] = valid[0] # Twitter handle
            
            # 3. Linktree / Bio Link Logic (Heuristic for lists)
            # If we see many links, prioritizing the first X link in a "socials" container is handled by the regex usually finding the first ones in nav/footer.
            
        except Exception as e:
            pass
            
        return socials
