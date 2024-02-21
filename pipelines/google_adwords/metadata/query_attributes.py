"""
format : fields1,<space>field2,<space>field3....
"""


campaign = """campaign.name, campaign.id, metrics.absolute_top_impression_percentage, metrics.active_view_cpm, metrics.active_view_ctr, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, metrics.all_conversions, metrics.average_cpv, metrics.average_cpm, metrics.average_cpe, metrics.average_cpc, metrics.average_cost, metrics.all_conversions_value_by_conversion_date, metrics.all_conversions_value, metrics.all_conversions_from_interactions_rate, metrics.all_conversions_by_conversion_date, metrics.average_page_views, metrics.average_time_on_site, metrics.bounce_rate, metrics.clicks, metrics.content_budget_lost_impression_share, metrics.content_impression_share, metrics.content_rank_lost_impression_share, metrics.conversions, metrics.conversions_by_conversion_date, metrics.conversions_from_interactions_rate, metrics.conversions_value, metrics.conversions_value_by_conversion_date, metrics.cost_micros, metrics.cost_per_all_conversions, metrics.cost_per_conversion, metrics.cost_per_current_model_attributed_conversion, metrics.cross_device_conversions, metrics.ctr, metrics.current_model_attributed_conversions, metrics.view_through_conversions, metrics.video_views, metrics.video_view_rate, metrics.interaction_event_types, metrics.video_quartile_p75_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p100_rate, metrics.value_per_current_model_attributed_conversion, metrics.value_per_conversions_by_conversion_date, metrics.value_per_conversion, metrics.value_per_all_conversions_by_conversion_date, metrics.value_per_all_conversions, metrics.top_impression_percentage, metrics.search_top_impression_share, metrics.search_rank_lost_top_impression_share, metrics.search_rank_lost_impression_share, metrics.search_rank_lost_absolute_top_impression_share, metrics.search_impression_share, metrics.search_exact_match_impression_share, metrics.search_click_share, metrics.search_budget_lost_top_impression_share, metrics.search_budget_lost_impression_share, metrics.search_budget_lost_absolute_top_impression_share, metrics.search_absolute_top_impression_share, metrics.relative_ctr, metrics.phone_through_rate, metrics.phone_impressions, metrics.percent_new_visitors, metrics.current_model_attributed_conversions_from_interactions_rate, metrics.current_model_attributed_conversions_from_interactions_value_per_interaction, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions_value_per_cost, metrics.engagement_rate, metrics.engagements, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, metrics.impressions, metrics.interaction_rate, metrics.interactions, metrics.invalid_click_rate, metrics.invalid_clicks, metrics.phone_calls, segments.date"""

ad_group = """ad_group.campaign, ad_group.base_ad_group, ad_group.id, ad_group.name, ad_group.status, segments.date, metrics.absolute_top_impression_percentage, metrics.active_view_cpm, metrics.active_view_ctr, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, metrics.all_conversions, metrics.all_conversions_by_conversion_date, metrics.all_conversions_from_interactions_rate, metrics.all_conversions_value, metrics.all_conversions_value_by_conversion_date, metrics.average_cost, metrics.average_cpc, metrics.average_cpe, metrics.average_cpm, metrics.average_cpv, metrics.average_page_views, metrics.average_time_on_site, metrics.bounce_rate, metrics.clicks, metrics.content_impression_share, metrics.content_rank_lost_impression_share, metrics.conversions, metrics.conversions_by_conversion_date, metrics.conversions_from_interactions_rate, metrics.conversions_value, metrics.conversions_value_by_conversion_date, metrics.cost_micros, metrics.cost_per_all_conversions, metrics.cost_per_conversion, metrics.cost_per_current_model_attributed_conversion, metrics.cross_device_conversions, metrics.ctr, metrics.current_model_attributed_conversions, metrics.current_model_attributed_conversions_value, metrics.engagement_rate, metrics.engagements, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, metrics.impressions, metrics.interaction_event_types, metrics.interaction_rate, metrics.interactions, metrics.percent_new_visitors, metrics.phone_calls, metrics.phone_impressions, metrics.phone_through_rate, metrics.relative_ctr, metrics.search_absolute_top_impression_share, metrics.search_budget_lost_absolute_top_impression_share, metrics.search_budget_lost_top_impression_share, metrics.search_exact_match_impression_share, metrics.search_impression_share, metrics.search_rank_lost_absolute_top_impression_share, metrics.search_rank_lost_impression_share, metrics.search_rank_lost_top_impression_share, metrics.search_top_impression_share, metrics.top_impression_percentage, metrics.value_per_all_conversions, metrics.value_per_all_conversions_by_conversion_date, metrics.value_per_conversion, metrics.value_per_conversions_by_conversion_date, metrics.value_per_current_model_attributed_conversion, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.view_through_conversions, metrics.video_views, metrics.video_view_rate, metrics.video_quartile_p75_rate"""

ad_group_ad = """ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.status, ad_group_ad.ad_group, metrics.view_through_conversions, metrics.video_views, metrics.video_view_rate, metrics.video_quartile_p75_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p100_rate, metrics.value_per_current_model_attributed_conversion, metrics.value_per_conversions_by_conversion_date, metrics.value_per_conversion, metrics.value_per_all_conversions_by_conversion_date, metrics.value_per_all_conversions, metrics.top_impression_percentage, metrics.percent_new_visitors, metrics.interactions, metrics.interaction_rate, metrics.interaction_event_types, metrics.impressions, metrics.gmail_secondary_clicks, metrics.gmail_saves, metrics.gmail_forwards, metrics.engagements, metrics.engagement_rate, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, metrics.ctr, metrics.cross_device_conversions, metrics.cost_per_current_model_attributed_conversion, metrics.cost_per_conversion, metrics.cost_per_all_conversions, metrics.cost_micros, metrics.conversions_value_by_conversion_date, metrics.conversions_value, metrics.conversions_from_interactions_rate, metrics.conversions_by_conversion_date, metrics.conversions, metrics.clicks, metrics.bounce_rate, metrics.average_time_on_site, metrics.average_cpv, metrics.average_page_views, metrics.average_cpm, metrics.average_cpe, metrics.average_cpc, metrics.average_cost, metrics.all_conversions_value_by_conversion_date, metrics.all_conversions_value, metrics.all_conversions_from_interactions_rate, metrics.all_conversions_by_conversion_date, metrics.all_conversions, metrics.active_view_viewability, metrics.active_view_measurable_impressions, metrics.active_view_measurable_cost_micros, metrics.active_view_measurability, metrics.active_view_impressions, metrics.active_view_ctr, metrics.active_view_cpm, metrics.absolute_top_impression_percentage, customer.id, customer.time_zone, segments.date"""