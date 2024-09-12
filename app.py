import duckdb
import pandas as pd
import streamlit as st


def get_image_number(file_path) -> str:
    file_name = file_path.name
    number = file_name.split("_")[1].split(".")[0]
    return number


def read_geojson_file(file_path) -> pd.DataFrame:
    df_raw = pd.read_json(file_path)

    assert list(df_raw.columns) == ["type", "features"], "columns are not as expected"

    df_json_normalize = pd.json_normalize(df_raw["features"]).rename(
        {"type": "feature_type"}, axis=1
    )
    df_clean = pd.concat([df_raw["type"], df_json_normalize], axis=1)

    # some files don't have the isLocked column, thus we create one so that we can merge the data together
    # also create a column to indicate whether we add this column artifically
    df_clean["has_is_locked_column_in_original_file"] = True
    if "properties.isLocked" not in list(df_clean.columns):
        df_clean["properties.isLocked"] = None
        df_clean["has_is_locked_column_in_original_file"] = False

    # some files don't have the properties.classification.names column, thus we create one so that we can merge the data together
    # also create a column to indicate whether we add this column artifically
    df_clean["has_properties_classification_names_column_in_original_file"] = True
    if "properties.classification.names" not in list(df_clean.columns):
        df_clean["properties.classification.names"] = None
        df_clean["has_properties_classification_names_column_in_original_file"] = False

    # sort the column such that they are in same order for us to combine
    df_clean = df_clean.reindex(sorted(df_clean.columns), axis=1)

    return df_clean


def create_dataframe_from_txt(file_path):
    """
    Reads a text file with mouse ID and image number ranges and returns a pandas DataFrame.

    Args:
        file_path (streamlit.UploadedFile): The uploaded text file.

    Returns:
        pandas.DataFrame: A DataFrame with columns 'mouse_id' and 'image_number'.
    """
    data = []
    content = file_path.getvalue().decode("utf-8").splitlines()
    for line in content:
        line = line.strip()
        if line:
            mouse_id, number_range = line.split()
            start, end = map(int, number_range.split("-"))
            for number in range(start, end + 1):
                data.append([mouse_id, number])

    df = pd.DataFrame(data, columns=["mouse_id", "image_number"])
    return df


geojson_files = st.file_uploader(label="geojson_file", accept_multiple_files=True, type="geojson")
mapping_file = st.file_uploader(label="mapping_file", accept_multiple_files=False, type="txt")

if geojson_files:
    geojson_df_list = []
    progress_bar = st.progress(0)
    for i, file in enumerate(geojson_files):
        image_number = get_image_number(file)
        df_tmp = read_geojson_file(file)
        df_tmp["image_number"] = image_number
        geojson_df_list.append(df_tmp)
        progress_bar.progress((i + 1) / len(geojson_files))

    df_combine = pd.concat(geojson_df_list, axis=0).reset_index(drop=True)

if mapping_file:
    df_mouse_mapping = create_dataframe_from_txt(mapping_file)

if mapping_file and geojson_files:
    final_query = """
    select
        m.mouse_id
        , c.image_number
        , count(*) as row_count
        , count(case when "properties.classification.name" = 'CD8' then 1 else null end) as cd8_count
        , count(case when "properties.classification.name" = 'CD4' then 1 else null end) as cd4_count
        , count(case when "properties.classification.name" = 'Foxp3' then 1 else null end) as foxp3_count
    from
        df_combine as c
    left join
        df_mouse_mapping as m
    on
        c.image_number = m.image_number
    where
        (
            ("properties.classification.name" = 'CD8' and "properties.measurements.Area µm^2" >= 25)
            or
            ("properties.classification.name" = 'CD4' and "properties.measurements.Area µm^2" >= 30)
            or
            ("properties.classification.name" = 'Foxp3' and "properties.measurements.Area µm^2" >= 20)
        )
        and "properties.classification.name" != 'Other'
    group by 1, 2
    order by 1, 2
    """
    df_image_final = duckdb.sql(final_query).df()

    st.write("show 1st 5 rows of the final output")
    st.write(df_image_final.head())
    csv = df_image_final.to_csv(index=False)

    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name="image_data.csv",
        mime="text/csv",
    )
