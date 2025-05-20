-- Marketing Campaign Data Integration & Performance Analysis
-- As part of a marketing analytics project, I developed a data integration and reporting solution focusing on
-- performance data from Facebook Ads and Google Ads.
-- Using SQL and CTEs, I merged and normalized data at the campaign and ad set levels to ensure consistency
-- across both platforms.
-- I collected key metrics such as spend, impressions, clicks, and conversion value on a daily basis to uncover
-- performance patterns.
-- I carried out data cleaning and transformation processes in PostgreSQL using DBeaver, and then
-- transferred the results to Tableau for visualization.
-- The final dataset enabled cross-platform ad performance comparison and ROI-focused analyses.

WITH combined_data AS (
    SELECT
        ad_date,
        COALESCE(split_part(split_part(url_parameters, 'utm_source=', 2), '&', 1), '0') AS utm_source,
        COALESCE(split_part(split_part(url_parameters, 'utm_medium=', 2), '&', 1), '0') AS utm_medium,
        CASE 
            WHEN LOWER(split_part(split_part(url_parameters, 'utm_campaign=', 2), '&', 1)) = 'nan' THEN NULL
            ELSE LOWER(split_part(split_part(url_parameters, 'utm_campaign=', 2), '&', 1))
        END AS utm_campaign,
        COALESCE(spend,0) AS spend, 
        COALESCE(impressions,0) AS impressions,
        COALESCE(clicks,0) AS clicks, 
        COALESCE(leads,0) AS leads,
        COALESCE(value,0) AS value,
        fadset.adset_name,
        fcampaign.campaign_name
    FROM facebook_ads_basic_daily
    JOIN public.facebook_adset AS fadset ON homeworks.facebook_ads_basic_daily.adset_id = fadset.adset_id 
    JOIN public.facebook_campaign AS fcampaign ON homeworks.facebook_ads_basic_daily.campaign_id = fcampaign.campaign_id

    UNION ALL

    SELECT 
        ad_date, 
        COALESCE(split_part(split_part(url_parameters, 'utm_source=', 2), '&', 1), '0') AS utm_source,
        COALESCE(split_part(split_part(url_parameters, 'utm_medium=', 2), '&', 1), '0') AS utm_medium,
        CASE 
            WHEN LOWER(split_part(split_part(url_parameters, 'utm_campaign=', 2), '&', 1)) = 'nan' THEN NULL
            ELSE LOWER(split_part(split_part(url_parameters, 'utm_campaign=', 2), '&', 1))
        END AS utm_campaign,
        COALESCE(spend,0), 
        COALESCE(impressions,0),
        COALESCE(clicks,0), 
        COALESCE(leads,0),
        COALESCE(value,0),
        adset_name,
        campaign_name
    FROM google_ads_basic_daily
)       

-- Aggregating data by date, campaign, and source to analyze performance metrics consistently across platforms
SELECT 
    ad_date,
    utm_campaign,
    utm_source,
    SUM(spend) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(value) AS total_value,

    -- Calculating CTR (Click Through Rate) to measure engagement efficiency
    SUM(clicks)*100.0 / 
        (CASE WHEN SUM(impressions) = 0 THEN NULL ELSE SUM(impressions) END) AS CTR,
    
    -- Calculating CPC (Cost Per Click) for cost efficiency insights
    SUM(spend) / 
        (CASE WHEN SUM(clicks) = 0 THEN NULL ELSE SUM(clicks) END) AS CPC,
    
    -- Calculating CPM (Cost Per Mille) to understand cost per thousand impressions
    SUM(spend) * 1000.0 / 
        (CASE WHEN SUM(impressions) = 0 THEN NULL ELSE SUM(impressions) END) AS CPM,
    
    -- Calculating ROMI (Return on Marketing Investment) to evaluate campaign profitability
    SUM(value)*100.0 / 
        (CASE WHEN SUM(spend) = 0 THEN NULL ELSE SUM(spend) END) AS ROMI
    
FROM combined_data
GROUP BY
    ad_date,
    utm_campaign,
    utm_source;
