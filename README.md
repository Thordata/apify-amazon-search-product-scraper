## Amazon Search & Product Scraper (Python + Playwright)

This Actor searches Amazon for given keywords and extracts product list data that is ready for analysis or export.

Typical use cases:

- Product research for marketplaces (e.g. `iphone 17 case`, `usb c hub`, `gaming chair`)
- Competitor and price monitoring across multiple Amazon marketplaces
- Feeding product lists into further analytics, dashboards, or LLM pipelines

### Input

The input is defined in `.actor/input_schema.json` and exposed in Apify Console:

- `keywords` (array of strings, **required**)  
  List of search keywords, e.g. `["iphone 17 case", "usb c hub"]`.
- `max_items_per_keyword` (integer, default `50`)  
  Maximum number of products to scrape for each keyword.
- `max_pages` (integer, default `3`)  
  Maximum number of result pages to crawl for each keyword (1–20).
- `country` (string, default `"US"`)  
  Amazon marketplace to target: one of `US`, `UK`, `DE`, `FR`, `JP`.
- `min_rating` (number, default `0`)  
  If > 0, products with rating lower than this value are filtered out.
- `min_reviews` (integer, default `0`)  
  If > 0, products with fewer reviews than this value are filtered out.
- `exclude_sponsored` (boolean, default `false`)  
  If `true`, sponsored products are excluded from the results.
- `fetch_details` (boolean, default `false`)  
  If `true`, the Actor will open product detail pages for the first `max_detail_items` results per keyword to enrich data (e.g. category path).
- `max_detail_items` (integer, default `5`)  
  Maximum number of products per keyword for which to open detail pages when `fetch_details` is enabled.

### Output

Results are pushed to the default dataset. Each item contains (non‑exhaustive):

- `keyword`, `country`, `pageIndex`
- `asin`, `title`, `brand`, `productUrl`, `imageUrl`
- `price`, `priceText`, `originalPriceText`, `currency`
- `rating`, `reviewsCount`, `isPrime`, `isSponsored`, `badges`
- Optional when `fetch_details=true`: `categoryPath`, `featureBullets`

The dataset view is configured in `.actor/dataset_schema.json` so that the **Overview** table shows the most important fields directly in Apify Console.

### Running locally

From the project root:

```bash
apify run
```

The default local input is stored in `storage/key_value_stores/default/INPUT.json`.  
You can edit it to test different keywords, marketplaces and filters.

### Anti‑bot considerations

Amazon employs strong anti‑bot protections. This Actor includes:

- Realistic desktop browser profile (user agent, viewport, locale)
- Short navigation timeouts with retries and basic backoff
- Simple detection of common CAPTCHA / bot‑check pages

For production runs on Apify, it is strongly recommended to:

- Enable Apify Proxy and use residential or high‑quality datacenter IPs
- Keep concurrency reasonable (e.g. 1–3 browser contexts) to avoid rate limits

### Future extensions

Planned improvements before public release:

- Additional fields (brand, badges, delivery info) when consistently available
- More robust selectors & fallbacks for Amazon UI changes
- Optional export helpers (e.g. sorting, filtering presets for typical workflows)

