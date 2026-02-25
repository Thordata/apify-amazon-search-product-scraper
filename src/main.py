"""Main entry point for the Apify Actor."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from apify import Actor
from playwright.async_api import BrowserContext, Locator, TimeoutError as PlaywrightTimeoutError, async_playwright

# NOTE:
# - This Actor searches Amazon by keywords and scrapes product list data.
# - Amazon has strong anti-bot protections. For production use on the Apify platform,
#   you should configure proxies and reasonable concurrency in the Actor settings.
# - Locally we focus on getting the basic search page parsing and data structure correct.


@dataclass
class AmazonSearchInput:
    """Normalized Actor input."""

    keywords: List[str]
    max_items_per_keyword: int
    max_pages: int
    country: str
    min_rating: Optional[float]
    min_reviews: Optional[int]
    exclude_sponsored: bool
    fetch_details: bool
    max_detail_items: int


def _normalize_input(raw: Dict[str, Any]) -> AmazonSearchInput:
    """Normalize and validate Actor input, filling default values."""
    keywords = raw.get('keywords') or []
    if isinstance(keywords, str):
        keywords = [keywords]

    keywords = [k.strip() for k in keywords if isinstance(k, str) and k.strip()]

    if not keywords:
        # Use current-generation flagship as default example to stay relevant.
        keywords = ['iphone 17 case']

    max_items_per_keyword = int(raw.get('max_items_per_keyword', 50) or 50)
    if max_items_per_keyword <= 0:
        max_items_per_keyword = 50

    max_pages = int(raw.get('max_pages', 3) or 3)
    if max_pages <= 0:
        max_pages = 1
    if max_pages > 20:
        max_pages = 20

    country = (raw.get('country') or 'US').upper()
    if country not in {'US', 'UK', 'DE', 'FR', 'JP'}:
        country = 'US'

    min_rating_val: Optional[float] = None
    if raw.get('min_rating') is not None:
        try:
            min_rating_val = float(raw['min_rating'])
        except (TypeError, ValueError):
            min_rating_val = None

    min_reviews_val: Optional[int] = None
    if raw.get('min_reviews') is not None:
        try:
            mr = int(raw['min_reviews'])
            min_reviews_val = mr if mr > 0 else None
        except (TypeError, ValueError):
            min_reviews_val = None

    exclude_sponsored = bool(raw.get('exclude_sponsored', False))

    fetch_details = bool(raw.get('fetch_details', False))
    max_detail_items = int(raw.get('max_detail_items', 5) or 5)
    if max_detail_items <= 0:
        max_detail_items = 1
    if max_detail_items > 50:
        max_detail_items = 50

    return AmazonSearchInput(
        keywords=keywords,
        max_items_per_keyword=max_items_per_keyword,
        max_pages=max_pages,
        country=country,
        min_rating=min_rating_val,
        min_reviews=min_reviews_val,
        exclude_sponsored=exclude_sponsored,
        fetch_details=fetch_details,
        max_detail_items=max_detail_items,
    )


def _country_to_domain(country: str) -> str:
    mapping = {
        'US': 'www.amazon.com',
        'UK': 'www.amazon.co.uk',
        'DE': 'www.amazon.de',
        'FR': 'www.amazon.fr',
        'JP': 'www.amazon.co.jp',
    }
    return mapping.get(country.upper(), 'www.amazon.com')


async def _parse_single_card(
    card: Locator,
    base_url: str,
    min_rating: Optional[float],
    min_reviews: Optional[int],
    exclude_sponsored: bool,
) -> Optional[Dict[str, Any]]:
    """Parse a single product card into a structured item."""
    try:
        # ASIN
        asin = await card.get_attribute('data-asin')
        if not asin:
            return None

        # Title (use a selector pattern that matches current Amazon layout,
        # with fallbacks for older variants).
        title_el = card.locator('a.a-link-normal.s-link-style.a-text-normal')
        if await title_el.count() == 0:
            title_el = card.locator('h2 a.a-link-normal')

        if await title_el.count() == 0:
            Actor.log.debug('Skipping card: no title link found')
            return None

        title = (await title_el.first.text_content() or '').strip()

        # Product detail URL
        href = await title_el.first.get_attribute('href')
        if not href:
            Actor.log.debug('Skipping card: title link has no href')
            return None
        if href.startswith('/'):
            product_url = f"{base_url}{href.split('?')[0]}"
        else:
            product_url = href.split('?')[0]

        # Price (use the offscreen text and also try to parse numeric value)
        price_locator = card.locator('span.a-price > span.a-offscreen')
        whole = ''
        if await price_locator.count() > 0:
            whole = (await price_locator.first.text_content() or '').strip()
        price = None
        if whole:
            price_text = whole
            # Normalize number for both US (1,234.56) and EU (1.234,56) styles.
            numeric_part = ''.join(ch if (ch.isdigit() or ch in ',.') else '' for ch in whole)
            if numeric_part:
                if ',' in numeric_part and '.' not in numeric_part:
                    # Likely EU style: 92,14 -> 92.14
                    normalized = numeric_part.replace('.', '').replace(',', '.')
                else:
                    # US style or mixed: 1,234.56 -> 1234.56
                    normalized = numeric_part.replace(',', '')
                try:
                    price = float(normalized)
                except ValueError:
                    price = None
        else:
            price_text = ''

        # Currency (best-effort: look at leading symbol or trailing ISO code).
        currency = ''
        if price_text:
            stripped = price_text.strip()
            if stripped and stripped[0] in '$€£¥':
                currency = stripped[0]
            else:
                last_token = stripped.split()[-1]
                if len(last_token) in {3, 4}:
                    currency = last_token

        # Original (striked-through) price
        original_price_locator = card.locator('span.a-price.a-text-price span.a-offscreen')
        original_price_text = ''
        if await original_price_locator.count() > 0:
            original_price_text = (await original_price_locator.first.text_content() or '').strip()

        # Rating and reviews count
        rating_locator = card.locator('span.a-icon-alt')
        rating_text = ''
        if await rating_locator.count() > 0:
            rating_text = (await rating_locator.first.text_content() or '').strip()
        rating_value: Optional[float] = None
        if rating_text:
            try:
                rating_value = float(rating_text.split()[0].replace(',', '.'))
            except (ValueError, IndexError):
                rating_value = None

        reviews_locator = card.locator('span.a-size-base.s-underline-text')
        reviews_text = ''
        if await reviews_locator.count() > 0:
            reviews_text = (await reviews_locator.first.text_content() or '').strip()
        reviews_count: Optional[int] = None
        if reviews_text:
            try:
                reviews_count = int(reviews_text.replace(',', '').replace('.', ''))
            except ValueError:
                reviews_count = None

        # Prime badge
        is_prime = await card.locator('i.a-icon.a-icon-prime, span[data-component-type=\"s-prime\"]').count() > 0

        # Brand (best-effort; may be missing for some results)
        brand = await card.get_attribute('data-brand') or ''
        brand = (brand or '').strip()
        if not brand:
            brand_locator = card.locator('h5.s-line-clamp-1 span, span.a-size-base-plus.a-color-base')
            if await brand_locator.count() > 0:
                brand = (await brand_locator.first.text_content() or '').strip()

        if brand:
            lowered = brand.lower()
            badge_like_keywords = [
                "amazon's choice",
                'overall pick',
                'best seller',
                'limited time deal',
            ]
            if any(k in lowered for k in badge_like_keywords):
                brand = ''

        # Badges / labels (e.g. Amazon's Choice, Best Seller)
        badge_locator = card.locator(
            'span.a-badge-text, span.s-label-popover-default, span.s-label-popover-default span.a-badge-label-inner'
        )
        badges: List[str] = []
        if await badge_locator.count() > 0:
            for i in range(await badge_locator.count()):
                text = await badge_locator.nth(i).text_content()
                if text:
                    cleaned = text.strip()
                    if cleaned and cleaned not in badges:
                        badges.append(cleaned)

        # Sponsored flag
        sponsored_locator = card.locator('span.s-sponsored-label-text, span.a-color-secondary')
        is_sponsored = False
        if await sponsored_locator.count() > 0:
            text = (await sponsored_locator.first.text_content() or '').strip().lower()
            if 'sponsored' in text:
                is_sponsored = True

        # Filters:
        if min_rating is not None and rating_value is not None and rating_value < min_rating:
            return None
        if min_reviews is not None and reviews_count is not None and reviews_count < min_reviews:
            return None
        if exclude_sponsored and is_sponsored:
            return None

        # Main image (take the first matching s-image to avoid strict-mode violations)
        image_locator = card.locator('img.s-image')
        image_url = ''
        if await image_locator.count() > 0:
            image_url = (await image_locator.first.get_attribute('src')) or ''

        return {
            'asin': asin,
            'title': title,
            'productUrl': product_url,
            'priceText': price_text,
            'price': price,
            'originalPriceText': original_price_text,
            'rating': rating_value,
            'reviewsCount': reviews_count,
            'isPrime': is_prime,
            'brand': brand,
            'badges': badges,
            'isSponsored': is_sponsored,
            'imageUrl': image_url,
            'currency': currency,
        }
    except Exception:
        Actor.log.debug('Failed to parse one product card', exc_info=True)
        return None


async def _extract_product_cards(
    card_locators: List[Locator],
    base_url: str,
    min_rating: Optional[float],
    min_reviews: Optional[int],
    exclude_sponsored: bool,
) -> List[Dict[str, Any]]:
    """Parse product information from search result cards."""
    items: List[Dict[str, Any]] = []

    for card in card_locators:
        try:
            item = await asyncio.wait_for(
                _parse_single_card(
                    card=card,
                    base_url=base_url,
                    min_rating=min_rating,
                    min_reviews=min_reviews,
                    exclude_sponsored=exclude_sponsored,
                ),
                timeout=5,  # seconds per card
            )
        except asyncio.TimeoutError:
            Actor.log.warning('Timed out while parsing a single product card, skipping it.')
            continue

        if item:
            items.append(item)

    return items


async def _scrape_keyword(
    context: BrowserContext,
    keyword: str,
    country: str,
    max_items: int,
    max_pages: int,
    min_rating: Optional[float],
    min_reviews: Optional[int],
    exclude_sponsored: bool,
    fetch_details: bool,
    max_detail_items: int,
) -> None:
    """Scrape search results for a single keyword and push data to the default dataset."""
    domain = _country_to_domain(country)
    base_url = f'https://{domain}'

    from urllib.parse import quote_plus

    search_url = f'{base_url}/s?k={quote_plus(keyword)}'

    Actor.log.info(f'Start scraping keyword=\"{keyword}\" from {search_url}')

    total_collected = 0
    page_index = 1

    while total_collected < max_items and page_index <= max_pages:
        page = await context.new_page()
        try:
            # Robust navigation with retries and shorter timeout.
            max_nav_retries = 3
            for attempt in range(1, max_nav_retries + 1):
                try:
                    await page.goto(
                        search_url,
                        wait_until='domcontentloaded',
                        timeout=20_000,
                    )
                    # Give the page a bit of time to render dynamic content.
                    await page.wait_for_timeout(2_000)
                    break
                except PlaywrightTimeoutError:
                    Actor.log.warning(
                        f'Navigation timeout for "{keyword}" page={page_index}, '
                        f'attempt {attempt}/{max_nav_retries}'
                    )
                    if attempt == max_nav_retries:
                        raise
                    # Exponential-ish backoff
                    sleep_ms = int(random.uniform(1_000, 3_000) * attempt)
                    await page.wait_for_timeout(sleep_ms)

            # Basic bot / captcha detection based on HTML markers.
            html_lower = (await page.content()).lower()
            captcha_markers = [
                'api-services-support@amazon.com',
                'to discuss automated access to amazon data',
                '/captcha/',
                'enter the characters you see below',
            ]
            if any(marker in html_lower for marker in captcha_markers):
                Actor.log.warning(
                    'This page looks like a bot-protection / CAPTCHA page. '
                    'No products will be parsed for this keyword.'
                )
                break

            cards = await page.locator('div.s-main-slot div[data-component-type=\"s-search-result\"]').all()
            Actor.log.info(f'Found {len(cards)} product cards on page {page_index}')

            if not cards:
                break

            # Only parse as many cards as we still need, to avoid wasting time on extra items.
            remaining = max_items - total_collected
            if remaining <= 0:
                break
            if len(cards) > remaining:
                cards = cards[:remaining]

            items = await _extract_product_cards(
                cards,
                base_url=base_url,
                min_rating=min_rating,
                min_reviews=min_reviews,
                exclude_sponsored=exclude_sponsored,
            )

            Actor.log.info(f'Parsed {len(items)} products from cards on page {page_index}')


            if not items:
                Actor.log.info('No valid products parsed from cards, stopping for this keyword.')
                break

            # Optionally enrich first N items with detail-page data (category path, etc.).
            if fetch_details and max_detail_items > 0:
                detail_count = 0
                for item in items:
                    if detail_count >= max_detail_items:
                        break
                    detail_url = item.get('productUrl')
                    if not detail_url:
                        continue
                    try:
                        detail_page = await context.new_page()
                        await detail_page.goto(detail_url, wait_until='domcontentloaded', timeout=20_000)
                        # Best-effort category / breadcrumb extraction.
                        breadcrumb_locator = detail_page.locator(
                            '#wayfinding-breadcrumbs_feature_div li a, '
                            'nav[aria-label="Breadcrumb"] a'
                        )
                        category_path: List[str] = []
                        if await breadcrumb_locator.count() > 0:
                            for i in range(await breadcrumb_locator.count()):
                                text = await breadcrumb_locator.nth(i).text_content()
                                if text:
                                    cleaned = text.strip()
                                    if cleaned:
                                        category_path.append(cleaned)
                        if category_path:
                            item['categoryPath'] = category_path

                        # Feature bullets (short description points).
                        bullets_locator = detail_page.locator('#feature-bullets ul li span')
                        feature_bullets: List[str] = []
                        if await bullets_locator.count() > 0:
                            for i in range(await bullets_locator.count()):
                                text = await bullets_locator.nth(i).text_content()
                                if text:
                                    cleaned = text.strip()
                                    if cleaned:
                                        feature_bullets.append(cleaned)
                        if feature_bullets:
                            item['featureBullets'] = feature_bullets
                        detail_count += 1
                    except Exception:
                        Actor.log.debug('Failed to enrich product with detail page', exc_info=True)
                    finally:
                        try:
                            await detail_page.close()
                        except Exception:
                            pass

            for item in items:
                await Actor.push_data(
                    {
                        'keyword': keyword,
                        'country': country,
                        'pageIndex': page_index,
                        **item,
                    }
                )

            total_collected += len(items)
            Actor.log.info(
                f'Pushed {len(items)} items for page {page_index}, '
                f'collected {total_collected}/{max_items} items for \"{keyword}\" so far'
            )

            if total_collected >= max_items:
                break

            next_btn = page.locator('a.s-pagination-next:not(.s-pagination-disabled)')
            if await next_btn.count() == 0:
                Actor.log.info('No more pages, stopping pagination.')
                break

            next_href = await next_btn.first.get_attribute('href')
            if not next_href:
                break

            if next_href.startswith('/'):
                search_url = f'{base_url}{next_href}'
            else:
                search_url = next_href

            page_index += 1
        except Exception:
            Actor.log.exception(f'Failed scraping keyword=\"{keyword}\" page={page_index}')
            break
        finally:
            await page.close()


async def main() -> None:
    """Entry point of the Amazon Search & Product Scraper Actor."""
    async with Actor:
        raw_input = await Actor.get_input() or {}
        parsed_input = _normalize_input(raw_input)

        Actor.log.info(
            f'Input parsed: keywords={parsed_input.keywords}, '
            f'max_items_per_keyword={parsed_input.max_items_per_keyword}, '
            f'max_pages={parsed_input.max_pages}, '
            f'country={parsed_input.country}, min_rating={parsed_input.min_rating}, '
            f'min_reviews={parsed_input.min_reviews}, exclude_sponsored={parsed_input.exclude_sponsored}, '
            f'fetch_details={parsed_input.fetch_details}, max_detail_items={parsed_input.max_detail_items}'
        )

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=Actor.configuration.headless,
                args=['--disable-gpu'],
            )

            # Use a realistic desktop browser profile to reduce basic bot detection
            # and adapt locale slightly per marketplace.
            locale_by_country = {
                'US': 'en-US',
                'UK': 'en-GB',
                'DE': 'de-DE',
                'FR': 'fr-FR',
                'JP': 'ja-JP',
            }
            locale = locale_by_country.get(parsed_input.country, 'en-US')

            context = await browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/121.0.0.0 Safari/537.36'
                ),
                locale=locale,
                viewport={'width': 1366, 'height': 768},
            )

            try:
                for keyword in parsed_input.keywords:
                    await _scrape_keyword(
                        context=context,
                        keyword=keyword,
                        country=parsed_input.country,
                        max_items=parsed_input.max_items_per_keyword,
                        max_pages=parsed_input.max_pages,
                        min_rating=parsed_input.min_rating,
                        min_reviews=parsed_input.min_reviews,
                        exclude_sponsored=parsed_input.exclude_sponsored,
                        fetch_details=parsed_input.fetch_details,
                        max_detail_items=parsed_input.max_detail_items,
                    )
            finally:
                await context.close()
                await browser.close()

