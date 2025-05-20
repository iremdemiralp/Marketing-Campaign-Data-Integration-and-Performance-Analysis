WITH joined AS (
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
    JOIN public.facebook_adset AS fadset ON facebook_ads_basic_daily.adset_id = fadset.adset_id 
    JOIN public.facebook_campaign AS fcampaign ON facebook_ads_basic_daily.campaign_id = fcampaign.campaign_id
    UNION ALL
    SELECT 
    	ad_date, 	
   		COALESCE(split_part(split_part(url_parameters, 'utm_source=', 2), '&', 1), '0') AS utm_source,
        COALESCE(split_part(split_part(url_parameters, 'utm_medium=', 2), '&', 1), '0') AS utm_medium,
    CASE 
  	 	WHEN LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^%&#$]+)')) = 'nan' THEN NULL
   		ELSE LOWER(SUBSTRING(url_parameters FROM 'utm_campaign=([^%&#$]+)'))
		END AS utm_campaign,
    	COALESCE(spend,0), 
 		COALESCE(impressions,0),
		COALESCE(clicks,0), 
		COALESCE(leads,0),
		COALESCE(value,0),
 	  	adset_name,
 	  	campaign_name
    FROM public.google_ads_basic_daily
    ) ,      
    combined_data2 AS (
  	SELECT 
    DISTINCT DATE_TRUNC('month', ad_date)::DATE AS ad_month,
    utm_campaign,
	SUM(spend) AS total_spend,
	SUM(impressions) AS total_impressions,
	SUM(clicks) AS total_clicks,
	SUM(value) AS conversion_value,
	SUM(clicks)*100.0 / 
    (CASE WHEN SUM(impressions) = 0 THEN NULL ELSE SUM(impressions) END) AS CTR,
	SUM(spend) / 
    (CASE WHEN SUM(clicks) = 0 THEN NULL ELSE SUM(clicks) END) AS CPC,
    SUM(spend) * 1000.0 / 
    (CASE WHEN SUM(impressions) = 0 THEN NULL ELSE SUM(impressions) END) AS CPM,
    SUM(value)*100.0 / 
    (CASE WHEN SUM(spend) = 0 THEN NULL ELSE SUM(spend) END) AS ROMI
    FROM joined
    GROUP BY  ad_month,
    utm_campaign
	)									
SELECT 
ad_month,
utm_campaign,
total_spend,
total_impressions,
total_clicks,
conversion_value,
CTR,CPC,CPM,ROMI, 
 	ROUND(
        CASE 
        WHEN LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) = 0 OR CTR = 0 THEN NULL
        ELSE ((CTR - LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) * 100 
        END, 2) AS CTR_percentage,     
    ROUND(
        CASE 
        WHEN LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) = 0 OR CPM = 0 THEN NULL
        ELSE ((CPM - LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) * 100 
        END, 2) AS CPM_percentage,
    ROUND(
        CASE 
        WHEN LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) = 0 OR ROMI = 0 THEN NULL
        ELSE ((ROMI - LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) * 100 
        END, 2) AS ROMI_percentage,
    ROUND(
        CASE 
        WHEN LAG(CPC) OVER (PARTITION BY utm_campaign ORDER BY ad_month) = 0 OR CPC = 0 THEN NULL
        ELSE ((CPC - LAG(CPC) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / LAG(CPC) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) * 100 
        END, 2) AS CPC_percentage
 FROM combined_data2  
 ORDER BY 1;