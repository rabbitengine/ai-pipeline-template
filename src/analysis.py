"""Query standardized campaign data."""

from google.cloud import bigquery
import google.auth

from config import load_config


def main():
    config = load_config()

    credentials, project = google.auth.default()
    client = bigquery.Client(
        credentials=credentials,
        project=config.destination.project or project
    )

    table_id = f"{client.project}.{config.destination.dataset}.{config.destination.table}"

    print("\n" + "=" * 70)
    print("  Campaign Name Standardization")
    print("=" * 70)

    # Show the problem
    print("\n  Before: Can't GROUP BY platform")
    print("  " + "-" * 66)
    print("  These are all Facebook campaigns:")
    query = f"SELECT campaign_name FROM `{table_id}` WHERE source = 'facebook'"
    for row in client.query(query).result():
        print(f"    {row.campaign_name}")
    print("\n  (fb_, Facebook -, meta|, FB_ — same platform, different names)")

    # Show the solution: aggregate by source
    print("\n  After: GROUP BY source")
    print("  " + "-" * 66)
    query = f"""
        SELECT
            source,
            SUM(spend) as spend,
            SUM(revenue) as revenue,
            ROUND(SUM(revenue) / SUM(spend), 2) as roas
        FROM `{table_id}`
        GROUP BY source
        ORDER BY spend DESC
    """
    print(f"  {'Platform':<12} {'Spend':>10} {'Revenue':>12} {'ROAS':>8}")
    print("  " + "-" * 44)
    for row in client.query(query).result():
        print(f"  {row.source:<12} ${row.spend:>9,.0f} ${row.revenue:>11,.0f} {row.roas:>7}x")

    # Aggregate by objective
    print("\n  By objective")
    print("  " + "-" * 66)
    query = f"""
        SELECT
            objective,
            SUM(spend) as spend,
            SUM(revenue) as revenue,
            ROUND(SUM(revenue) / SUM(spend), 2) as roas
        FROM `{table_id}`
        WHERE objective IS NOT NULL
        GROUP BY objective
        ORDER BY roas DESC
    """
    print(f"  {'Objective':<15} {'Spend':>10} {'Revenue':>12} {'ROAS':>8}")
    print("  " + "-" * 47)
    for row in client.query(query).result():
        print(f"  {row.objective:<15} ${row.spend:>9,.0f} ${row.revenue:>11,.0f} {row.roas:>7}x")

    # Aggregate by geo
    print("\n  By geo")
    print("  " + "-" * 66)
    query = f"""
        SELECT
            geo,
            SUM(spend) as spend,
            SUM(revenue) as revenue,
            ROUND(SUM(revenue) / SUM(spend), 2) as roas
        FROM `{table_id}`
        GROUP BY geo
        ORDER BY roas DESC
    """
    print(f"  {'Geo':<6} {'Spend':>10} {'Revenue':>12} {'ROAS':>8}")
    print("  " + "-" * 38)
    for row in client.query(query).result():
        print(f"  {row.geo:<6} ${row.spend:>9,.0f} ${row.revenue:>11,.0f} {row.roas:>7}x")

    print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    main()
