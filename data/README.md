# Data

## Olist Brazilian E-Commerce Dataset

Source: [Hugging Face mirror](https://huggingface.co/datasets/miminmoons/olist-ecommerce-for-delivery-and-review-prediction)

The CSVs are **not committed** to this repo (too large). To download them:

```powershell
cd data/raw
$base = "https://huggingface.co/datasets/miminmoons/olist-ecommerce-for-delivery-and-review-prediction/resolve/main/data"
$files = @(
  "olist_customers_dataset.csv",
  "olist_geolocation_dataset.csv",
  "olist_order_items_dataset.csv",
  "olist_order_payments_dataset.csv",
  "olist_order_reviews_dataset.csv",
  "olist_orders_dataset.csv",
  "olist_products_dataset.csv",
  "olist_sellers_dataset.csv",
  "product_category_name_translation.csv"
)
foreach ($f in $files) { Invoke-WebRequest "$base/$f" -OutFile $f }
```

## Files

| File | Size (approx) | Purpose |
|---|---|---|
| olist_customers_dataset.csv | 8.6 MB | Customer info per order |
| olist_geolocation_dataset.csv | 45.7 MB | ZIP → lat/lng lookup |
| olist_order_items_dataset.csv | 14.7 MB | Line items per order |
| olist_order_payments_dataset.csv | 5.5 MB | Payments per order |
| olist_order_reviews_dataset.csv | 13.8 MB | Reviews per order |
| olist_orders_dataset.csv | 16.8 MB | Order header + status |
| olist_products_dataset.csv | 2.3 MB | Product catalog |
| olist_sellers_dataset.csv | 0.2 MB | Seller catalog |
| product_category_name_translation.csv | ~2 KB | Portuguese → English category names |

## Notes

- Sizes reflect the Hugging Face mirror snapshot, which may differ slightly from the original Kaggle release.
- The geolocation file is significantly deduplicated compared to some Kaggle versions.
- All schemas (column names, types) match the canonical Olist dataset.