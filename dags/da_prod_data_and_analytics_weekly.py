from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime

with DAG(dag_id='da_prod_data_and_analytics_weekly', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    da_prod_sotday_weekly_task = BashOperator(task_id='da_prod_sotday_weekly', bash_command='echo "Start of the day- my ecsd var: ECSD - SLACK_DA"', owner='ctmuser', doc='Start of the day job for weekly D&A scehdule')
    da_prod_ctm_01_fb_pen_firstscans_task = BashOperator(task_id='da_prod_ctm_01_fb_pen_firstscans', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   flybuys_Metrics_ISU_Scans_by_Partner_ISU_Scans_by_Store/01_fb_firstscans_CURRENT.r', owner='ctmuser', doc='Run the FB_Penetration first scans job')
    da_prod_ctm_03_fb_pen_store_ops_task = BashOperator(task_id='da_prod_ctm_03_fb_pen_store_ops', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   flybuys_Metrics_ISU_Scans_by_Partner_ISU_Scans_by_Store/03_fb_StoreOpsReport_CURRENT.r', owner='ctmuser', doc='Run the FB_Penetration store Ops Job')
    da_prod_ctm_05_fb_store_data_export_task = BashOperator(task_id='da_prod_ctm_05_fb_store_data_export', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   flybuys_Metrics_ISU_Scans_by_Partner_ISU_Scans_by_Store/05_fb_store_data_export.r', owner='ctmuser', doc='Run the fb store - TM1 finance data export Weekly Job')
    da_prod_ctm_rfm_liquorland_scoring_main_task = BashOperator(task_id='da_prod_ctm_rfm_liquorland_scoring_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_Liquorland/LOAD_PR0191_LIQL_RFM_SCORING.SQL', owner='ctmuser', doc='This program refreshes the Liquorland RFM scoring data')
    da_prod_ctm_rfm_liquorland_scoring_mail_notification_task = BashOperator(task_id='da_prod_ctm_rfm_liquorland_scoring_mail_notification', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_Liquorland/RFMLiquorlandScoring_MailNotif_CURRENT.r', owner='ctmuser', doc='This program refreshes the Liquorland RFM scoring data- mail notification')
    da_prod_ctm_rfm_kmart_scoring_main_task = BashOperator(task_id='da_prod_ctm_rfm_kmart_scoring_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_Kmart/LOAD_PR0191_KMART_RFM_SCORING.SQL', owner='ctmuser', doc='This program refreshes the Kmart RFM scoring data')
    da_prod_ctm_rfm_kmart_scoring_mail_notification_task = BashOperator(task_id='da_prod_ctm_rfm_kmart_scoring_mail_notification', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_Kmart/RFMKmartScoring_MailNotif_CURRENT.r', owner='ctmuser', doc='This program refreshes the Kmart RFM scoring data- mail notification')
    da_prod_ctm_rfm_target_scoring_main_task = BashOperator(task_id='da_prod_ctm_rfm_target_scoring_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_Target/LOAD_PR0191_TARGET_RFM_SCORING.SQL', owner='ctmuser', doc='This program refreshes the Target RFM scoring data- main')
    da_prod_ctm_rfm_target_scoring_mail_notification_task = BashOperator(task_id='da_prod_ctm_rfm_target_scoring_mail_notification', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_Target/RFMTargetScoring_MailNotif_CURRENT.r', owner='ctmuser', doc='This program refreshes the Target RFM scoring data- mail notification')
    da_prod_ctm_rfm_first_choice_lm_scoring_main_task = BashOperator(task_id='da_prod_ctm_rfm_first_choice_lm_scoring_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_First_Choice/LOAD_PR0191_FCLM_RFM_SCORING.SQL', owner='ctmuser', doc='This program refreshes the  First Choice RFM scoring data- main')
    da_prod_ctm_rfm_first_choice_lm_scoring_mail_notification_task = BashOperator(task_id='da_prod_ctm_rfm_first_choice_lm_scoring_mail_notification', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   RFM_First_Choice/RFMFirstChoiceLMScoring_MailNotif_CURRENT.r', owner='ctmuser', doc='This program refreshes the  First Choice RFM scoring data- mail notification')
    da_prod_ctm_flybuys_mbr_day_preference_main_task = BashOperator(task_id='da_prod_ctm_flybuys_mbr_day_preference_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Day_Preference_V1/LOAD_MBR_DAY_PREFERENCE.sql', owner='ctmuser', doc='This program refreshes the members day preference data- main')
    da_prod_ctm_flybuys_mbr_day_preference_mail_notification_task = BashOperator(task_id='da_prod_ctm_flybuys_mbr_day_preference_mail_notification', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Day_Preference_V1/flybuysMbrDayPreference_MailNotif_CURRENT.R', owner='ctmuser', doc='This program refreshes the members day preference data- mail notification')
    da_prod_ctm_flybuys_program_dashboard_task = BashOperator(task_id='da_prod_ctm_flybuys_program_dashboard', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   flybuys_Program_Dashboard/flybuys_program_dashboard_CURRENT.r', owner='ctmuser', doc='Run the FLYBUYS_PROGRAM_DASHBOARD')
    da_prod_ctm_new_mini_isu_range_reminder_task = BashOperator(task_id='da_prod_ctm_new_mini_isu_range_reminder', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Mini_ISU_Reminder/New_Mini_ISU_range_CURRENT.r', owner='ctmuser', doc='Run the NEW_MINI_ISU_RANGE reminder email job')
    da_prod_ctm_fb_emailable_report_main_task = BashOperator(task_id='da_prod_ctm_fb_emailable_report_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Emailable_Members_Report/emailable_report_CURRENT.r', owner='ctmuser', doc='Run the FB Emailable member Report main job')
    da_prod_ctm_licensee_reminder_main_task = BashOperator(task_id='da_prod_ctm_licensee_reminder_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Weekly_Licensee_Master_Reminder_V1/PGM_Weekly_Licensee_Reminder_Main_CURRENT.r', owner='ctmuser', doc='Run the weekly licensee reminder main script')
    da_prod_ctm_licensee_reminder_chk_log_task = BashOperator(task_id='da_prod_ctm_licensee_reminder_chk_log', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Weekly_Licensee_Master_Reminder_V1/PGM_Weekly_Licensee_Reminder_Chk_Log_CURRENT.r', owner='ctmuser', doc='Run the weekly licensee reminder check log script')
    da_prod_ctm_licensee_reminder_email_task = BashOperator(task_id='da_prod_ctm_licensee_reminder_email', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   Weekly_Licensee_Master_Reminder_V1/PGM_Weekly_Licensee_Reminder_Email_CURRENT.R', owner='ctmuser', doc='Run the weekly licensee reminder mail notification script')
    da_prod_ctm_fb_new_members_weekly_main_task = BashOperator(task_id='da_prod_ctm_fb_new_members_weekly_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   New_Members_Report/New_Members_Report_fb_weekly_members_3_CURRENT.r', owner='ctmuser', doc='Run the FB_New_Members weekly main job')
    da_prod_ctm_liquor_yo_y_sales_report_task = BashOperator(task_id='da_prod_ctm_liquor_yo_y_sales_report', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py               -p R -r gitlab.com/flybuys_analytics/software_redevelopment/part.git -s                 Liquor_YoY_Sales_Dashboard/LQ_YoY_Sales_Report.r', owner='ctmuser', doc='Creating Liquor YoY Sales Report')
    da_prod_ctm_customer_levers_report_main_task = BashOperator(task_id='da_prod_ctm_customer_levers_report_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py                               -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s                                 Customer_Levers_Report/Customer_Levers_Report_Main_CURRENT.r', owner='ctmuser', doc='This program refreshes Customer Lever Report')
    da_prod_ctm_customer_levers_report_e_mail_notification_task = BashOperator(task_id='da_prod_ctm_customer_levers_report_e_mail_notification', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py                             -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s                               Customer_Levers_Report/Customer_Levers_Report_eMail_Notification_CURRENT.r', owner='ctmuser', doc='This program sends email notification for Customer Lever Report')
    da_prod_ctm_coles_online_report_task = BashOperator(task_id='da_prod_ctm_coles_online_report', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py                             -p R -r gitlab.com/flybuys_analytics/software_redevelopment/cafb.git -s                               Coles_Online_Report/All_flybuys_member_DC_NonDC_Subscribers.r', owner='ctmuser', doc='Run the weekly Coles Online Report')
    da_prod_ctm_liquor_bulk_purchase_task = BashOperator(task_id='da_prod_ctm_liquor_bulk_purchase', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/part.git -s   Liquor_Bulk_Purchase_Table/Lq_Bulk_Purchase.r', owner='ctmuser', doc='Run the Liquor Bulk Purchase Weekly Job')
    da_prod_ctm_rewards_dashboard_data_refresh_task = BashOperator(task_id='da_prod_ctm_rewards_dashboard_data_refresh', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py                             -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s                                  Rewards_Dashboard/Rewards_Dashboard.r', owner='ctmuser', doc='Run the Rewards Dashboard Refresh Job')
    da_prod_ctm_load_tell_kmart_survey_job_task = BashOperator(task_id='da_prod_ctm_load_tell_kmart_survey_job', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/kmart/data_marts.git -s   tell_kmart_survey/LOAD_KM070_TELLKMART_SURVEY.sql', owner='ctmuser', doc='Run LOAD_KM070_TELLKMART_SURVEY.sql - Weekly Kmart Survey Job')
    da_prod_ctm_coles_unique_qualification_tracker_task = BashOperator(task_id='da_prod_ctm_coles_unique_qualification_tracker', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/cafb.git -s   Coles_Unique_Qualifiers_Tracker/Unique_Qualifiers_Tracker_Main_Current.r', owner='ctmuser', doc='Run CTM_Coles_Unique_Qualification_Tracker - Weekly Job')
    da_prod_ctm_coles_td_unique_qualifier_dashboard_task = BashOperator(task_id='da_prod_ctm_coles_td_unique_qualifier_dashboard', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/cafb.git -s       Coles_TD_Unique_Participation_Dashboard/Coles_TD_Unique_Qualifier_Dashboard.r', owner='ctmuser', doc='Run CTM_Coles_TD_Unique_Qualifier_Dashboard - Weekly Job')
    da_prod_ctm_coles_nps_focus_group_main_task = BashOperator(task_id='da_prod_ctm_coles_nps_focus_group_main', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/cafb.git -s   Coles_NPS_Focus_Group/NPS_Focus_Group_Main_Current.r', owner='ctmuser', doc='Run CTM_Coles_NPS_Focus_Group_Main - Weekly Job')
    da_prod_ctm_coles_isu_first_scan_task = BashOperator(task_id='da_prod_ctm_coles_isu_first_scan', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/part.git -s   Coles_FScan_PBI_Report/Coles_ISU_FS_R_Script.r', owner='ctmuser', doc='Run CTM_Coles_ISU_First_Scan - Weekly Job')
    da_prod_ctm_ob_atl_tracker_june_2022_weekly_task = BashOperator(task_id='da_prod_ctm_ob_atl_tracker_june_2022_weekly', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/cafb.git -s         OB_x3_June_2022_Campaign_ATL/CA0356_Own_Brand_Weekly_Spreadsheet_DYNAMIC_TABLES.sql', owner='ctmuser', doc='Refresh tables for Weekly June 2022 Own Brand Tracker for Coles')
    da_prod_ctm_lq_mbr_base_task = BashOperator(task_id='da_prod_ctm_lq_mbr_base', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p Python -r gitlab.com/flybuys_analytics/software_redevelopment/PART.git -s         Liquor_Member_Base/LOAD_LQ_MBR_BASE.SQL', owner='ctmuser', doc='Run CTM_LQ_Mbr_Base stored procedure - Weekly Job')
    da_prod_ctm_wes_weekly_report_data_refresh_task = BashOperator(task_id='da_prod_ctm_wes_weekly_report_data_refresh', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s   WES/WES_Weekly_Report/WES_Weekly_Report.r', owner='ctmuser', doc='Run the WES Weekly Report Data Refresh')
    da_prod_ctm_consecutive_hitrate_segment_trigger_task = BashOperator(task_id='da_prod_ctm_consecutive_hitrate_segment_trigger', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s         Coles_Trigger/Consecutive_Hitrate_Segment_Trigger.r', owner='ctmuser', doc='Run the job - Consecutive Hitrate Segment Trigger')
    da_prod_ctm_wes_data_mart_report_task = BashOperator(task_id='da_prod_ctm_wes_data_mart_report', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/kmart/wespartners-data-mart.git -s           WES_DATAMART.r', owner='ctmuser', doc='Run WES DataMart Report')
    da_prod_ctm_rewards_automated_banners_version2_task = BashOperator(task_id='da_prod_ctm_rewards_automated_banners_version2', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/prog.git -s           Rewards/REWARDS_AUTOMATED_BANNERS/REWARDS_AUTOBANNER_VERSION2.R', owner='ctmuser', doc='Run REWARDS_AUTOMATED_BANNERS_VERSION2')
    da_prod_ctm_01_fb_pen_firstscans_task >> da_prod_ctm_03_fb_pen_store_ops_task
    da_prod_ctm_03_fb_pen_store_ops_task >> da_prod_ctm_05_fb_store_data_export_task
    da_prod_ctm_customer_levers_report_main_task >> da_prod_ctm_customer_levers_report_e_mail_notification_task
    da_prod_ctm_flybuys_mbr_day_preference_main_task >> da_prod_ctm_flybuys_mbr_day_preference_mail_notification_task
    da_prod_ctm_licensee_reminder_chk_log_task >> da_prod_ctm_licensee_reminder_email_task
    da_prod_ctm_licensee_reminder_main_task >> da_prod_ctm_licensee_reminder_chk_log_task
    da_prod_ctm_rfm_first_choice_lm_scoring_main_task >> da_prod_ctm_rfm_first_choice_lm_scoring_mail_notification_task
    da_prod_ctm_rfm_kmart_scoring_main_task >> da_prod_ctm_rfm_kmart_scoring_mail_notification_task
    da_prod_ctm_rfm_liquorland_scoring_main_task >> da_prod_ctm_rfm_liquorland_scoring_mail_notification_task
    da_prod_ctm_rfm_target_scoring_main_task >> da_prod_ctm_rfm_target_scoring_mail_notification_task
    da_prod_sotday_weekly_task >> da_prod_ctm_coles_nps_focus_group_main_task
    da_prod_sotday_weekly_task >> da_prod_ctm_coles_td_unique_qualifier_dashboard_task
    da_prod_sotday_weekly_task >> da_prod_ctm_coles_unique_qualification_tracker_task
    da_prod_sotday_weekly_task >> da_prod_ctm_consecutive_hitrate_segment_trigger_task
    da_prod_sotday_weekly_task >> da_prod_ctm_licensee_reminder_main_task
    da_prod_sotday_weekly_task >> da_prod_ctm_load_tell_kmart_survey_job_task
    da_prod_sotday_weekly_task >> da_prod_ctm_lq_mbr_base_task
    da_prod_sotday_weekly_task >> da_prod_ctm_new_mini_isu_range_reminder_task
    da_prod_sotday_weekly_task >> da_prod_ctm_ob_atl_tracker_june_2022_weekly_task
    da_prod_sotday_weekly_task >> da_prod_ctm_wes_data_mart_report_task