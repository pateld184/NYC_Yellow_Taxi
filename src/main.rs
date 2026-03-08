use datafusion::arrow::util::pretty;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a new DataFusion session
    let ctx = SessionContext::new();

    // Register all Parquet files in data/nyc_yellow_taxi_2025 as a single table "trips"
    ctx.register_parquet(
        "trips",
        "data/nyc_yellow_taxi_2025",
        ParquetReadOptions::default(),
    )
    .await?;

    println!("✅ Loaded NYC Yellow Taxi 2025 parquet files into table 'trips'");

    // === Inspect schema and pick a pickup datetime column ===
    let trips_df = ctx.table("trips").await?;
    let schema = trips_df.schema();

    println!("\nAvailable columns in 'trips' table:");
    for field in schema.fields() {
        println!("  - {}", field.name());
    }

    // Try to find a sensible pickup datetime column by name first
    let mut chosen: Option<String> = None;

    for f in schema.fields() {
        let name = f.name().to_lowercase();

        // Common TLC-style names
        if name == "tpep_pickup_datetime"
            || name == "lpep_pickup_datetime"
            || name == "pickup_datetime"
        {
            chosen = Some(f.name().to_string());
            break;
        }

        // More generic heuristic: something with "pickup" and ("time" or "date")
        if name.contains("pickup") && (name.contains("time") || name.contains("date")) {
            chosen = Some(f.name().to_string());
            break;
        }
    }

    // Fallback: if nothing matched, just use the first column in the schema
    let pickup_col_name = match chosen {
        Some(name) => {
            println!("\nUsing '{}' as the pickup datetime column.\n", name);
            name
        }
        None => {
            let fallback = schema
                .fields()
                .get(0)
                .expect("Schema has no columns at all")
                .name()
                .to_string();
            println!("\nNo obvious pickup datetime column found; defaulting to '{}'.\n", fallback);
            fallback
        }
    };

    // ---------------------------------------------------------------------
    // Aggregation 1: Trips and revenue by month (DataFrame API)
    // ---------------------------------------------------------------------

    let agg1_df = ctx
        .table("trips")
        .await?
        .select(vec![
            date_trunc(lit("month"), col(&pickup_col_name)).alias("pickup_month"),
            col("fare_amount"),
            col("total_amount"),
        ])?
        .aggregate(
            vec![col("pickup_month")],
            vec![
                count(lit(1)).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?; // ASC

    print_df(
        "Aggregation 1 - DataFrame API (Trips and revenue by month)",
        agg1_df,
    )
    .await?;

    // ---------------------------------------------------------------------
    // Aggregation 1: Trips and revenue by month (SQL)
    // ---------------------------------------------------------------------

    let agg1_sql = format!(
        r#"
        SELECT
            date_trunc('month', {pickup}) AS pickup_month,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM trips
        GROUP BY pickup_month
        ORDER BY pickup_month ASC
    "#,
        pickup = pickup_col_name
    );

    let agg1_sql_df = ctx.sql(&agg1_sql).await?;
    print_df(
        "Aggregation 1 - SQL (Trips and revenue by month)",
        agg1_sql_df,
    )
    .await?;

    // ---------------------------------------------------------------------
    // Aggregation 2: Tip behavior by payment type (DataFrame API)
    // ---------------------------------------------------------------------

    let agg2_df = ctx
        .table("trips")
        .await?
        .aggregate(
            vec![col("payment_type")],
            vec![
                count(lit(1)).alias("trip_count"),
                avg(col("tip_amount")).alias("avg_tip_amount"),
                (sum(col("tip_amount")) / sum(col("total_amount"))).alias("tip_rate"),
            ],
        )?
        .sort(vec![col("trip_count").sort(false, true)])?; // DESC

    print_df(
        "Aggregation 2 - DataFrame API (Tip behavior by payment type)",
        agg2_df,
    )
    .await?;

    // ---------------------------------------------------------------------
    // Aggregation 2: Tip behavior by payment type (SQL)
    // ---------------------------------------------------------------------

    let agg2_sql = r#"
        SELECT
            payment_type,
            COUNT(*) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / SUM(total_amount) AS tip_rate
        FROM trips
        GROUP BY payment_type
        ORDER BY trip_count DESC
    "#;

    let agg2_sql_df = ctx.sql(agg2_sql).await?;
    print_df(
        "Aggregation 2 - SQL (Tip behavior by payment type)",
        agg2_sql_df,
    )
    .await?;

    println!("\n✅ All aggregations completed successfully.");

    Ok(())
}

// Helper: collect and pretty-print a DataFrame with a title
async fn print_df(title: &str, df: DataFrame) -> datafusion::error::Result<()> {
    println!("\n==============================");
    println!("{title}");
    println!("==============================");

    let batches = df.collect().await?;
    pretty::print_batches(&batches)?;

    Ok(())
}