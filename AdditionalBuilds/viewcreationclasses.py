from pyspark import spark

class Table:


    @classmethod
    def create_global_view(cls):
        '''
        Create the global view based on the gold table.

        Note:
            Default view removes Enums and GUID (except Primary Key GUID).
        '''

        schema_for_view = f"dev_global_views"

        # Ensure Schema for global view exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_for_view}")

        if cls.custom_global_view_query is None:
            # Build default query
            def column_select(field: str) -> str:
                alias_creation = sub('([A-Z])', ' \\1', field).strip()
                return f"`{field}` AS `{alias_creation}`"

            def column_filter(field: str) -> bool:
                return (not field.endswith("Enum")) and (not field.endswith("GUID"))

            fields_for_query = ["Id", cls.source_guid] + [column_select(field) for field, _ in cls.fields if column_filter(field)]

            fields_query = ", ".join(fields_for_query)

            live_filter = 'WHERE IsLive' if cls.include_islive else ''

            creation_query = f"CREATE OR REPLACE VIEW {schema_for_view}.{cls.table_name}_global AS SELECT {fields_query} FROM {cls.schema_name}.{cls.table_name} {live_filter}"
        else:
            # Use provided query
            creation_query = f"CREATE OR REPLACE VIEW {schema_for_view}.{cls.table_name}_global AS {cls.custom_global_view_query}"

        spark.sql(creation_query)
        print(f"\nCreating custom global view\n\n{creation_query}")

    @classmethod
    def remove_global_view_if_exists(cls):
        '''
        Removes global view if it exists
        '''

        global_view = f"dev_global_views.{cls.table_name}_global"

        spark.sql(f"DROP VIEW IF EXISTS {global_view}")
        print(f"\nRemoving global view if exists\n\n{global_view}")