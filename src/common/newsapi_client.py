import datetime as dt

import requests

NEWSAPI_URL = "https://newsapi.org/v2/everything"

# NewsAPI's free/developer plan caps results at 100 per query (across all pages).
MAX_RESULTS = 100


def fetch_tesla_articles(
    api_key: str,
    query: str = "Tesla",
    days_back: int = 30,
    page_size: int = 100,
) -> dict:
    """Call NewsAPI for articles matching `query` from the last `days_back` days,
    sorted by most recent first. Paginates until all available results are
    collected or the plan's result cap is hit.
    """
    from_date = (dt.datetime.utcnow() - dt.timedelta(days=days_back)).strftime("%Y-%m-%d")

    all_articles: list[dict] = []
    page = 1
    total_results = 0

    while True:
        params = {
            "q": query,
            "from": from_date,
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": page_size,
            "page": page,
            "apiKey": api_key,
        }
        resp = requests.get(NEWSAPI_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("status") != "ok":
            raise RuntimeError(f"NewsAPI error: {payload}")

        total_results = payload.get("totalResults", 0)
        articles = payload.get("articles", [])
        all_articles.extend(articles)

        if not articles or len(all_articles) >= total_results:
            break
        if page * page_size >= MAX_RESULTS:
            break
        page += 1

    return {
        "status": "ok",
        "totalResults": total_results,
        "articles": all_articles,
        "fetchedAt": dt.datetime.utcnow().isoformat() + "Z",
        "query": query,
        "fromDate": from_date,
    }
