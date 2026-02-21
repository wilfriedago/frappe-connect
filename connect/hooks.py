app_name = "connect"
app_title = "Connect"
app_publisher = "BFT Group"
app_description = "Kafka Integration Bridge for Frappe/ERPNext"
app_icon = "octicon octicon-plug"
app_color = "#2E86C1"
app_email = "dev@bftgroup.com"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "connect",
# 		"logo": "/assets/connect/logo.png",
# 		"title": "Connect",
# 		"route": "/connect",
# 		"has_permission": "connect.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
app_include_css = "/assets/connect/css/connect.css"
# app_include_js = "/assets/connect/js/connect.js"

# include js, css files in header of web template
# web_include_css = "/assets/connect/css/connect.css"
# web_include_js = "/assets/connect/js/connect.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "connect/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
doctype_js = {
    "Connect Settings": "public/js/connect_settings.js",
    "Connect Emission Rule": "public/js/connect_emission_rule.js",
    "Connect Message Log": "public/js/connect_message_log.js",
}
doctype_list_js = {
    "Connect Message Log": "public/js/connect_message_log_list.js",
}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "connect/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# automatically load and sync documents of this doctype from downstream apps
# importable_doctypes = [doctype_1]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "connect.utils.jinja_methods",
# 	"filters": "connect.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "connect.install.before_install"
# after_install = "connect.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "connect.uninstall.before_uninstall"
# after_uninstall = "connect.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "connect.utils.before_app_install"
# after_app_install = "connect.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "connect.utils.before_app_uninstall"
# after_app_uninstall = "connect.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "connect.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# Document Events
# ---------------
# Hook on document methods and events

doc_events = {
    "*": {
        "after_insert": "connect.services.producer_service.on_document_event",
        "on_update": "connect.services.producer_service.on_document_event",
        "on_submit": "connect.services.producer_service.on_document_event",
        "on_cancel": "connect.services.producer_service.on_document_event",
        "on_trash": "connect.services.producer_service.on_document_event",
    }
}

# Scheduled Tasks
# ---------------

scheduler_events = {
    # "all": [
    # 	"connect.tasks.all"
    # ],
    # Cleanup old Kafka logs daily at midnight
    "daily": [
        "connect.jobs.cleanup.cleanup_kafka_logs",
    ],
    # "hourly": [
    # 	"connect.tasks.hourly"
    # ],
    # "weekly": [
    # 	"connect.tasks.weekly"
    # ],
    # "monthly": [
    # 	"connect.tasks.monthly"
    # ],
    # Refresh schema cache every 6 hours
    "cron": {
        "0 */6 * * *": [
            "connect.services.schema_service.refresh_schema_cache",
        ],
    },
}

# Testing
# -------

# before_tests = "connect.install.before_tests"

# Extend DocType Class
# ------------------------------
#
# Specify custom mixins to extend the standard doctype controller.
# extend_doctype_class = {
# 	"Task": "connect.custom.task.CustomTaskMixin"
# }

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "connect.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "connect.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["connect.utils.before_request"]
# after_request = ["connect.utils.after_request"]

# Job Events
# ----------
# before_job = ["connect.utils.before_job"]
# after_job = ["connect.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"connect.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

# Translation
# ------------
# List of apps whose translatable strings should be excluded from this app's translations.
# ignore_translatable_strings_from = []
