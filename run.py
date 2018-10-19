
# Input TSV format
# storage_site     project_code    song_analysis_id        xml_object_id   xml_file_name   xml_file_size   correct_xml_md5sum      song_xml_md5sum

#Score output TSV format (for manifest
#repo_code       file_id object_id       file_format     file_name       file_size       md5_sum index_object_id donor_id/donor_count    project_id/project_count        study


import pandas as pd
import os
import math

def convert (input_linda_tsv_filename, sep='\t'):
    df = pd.read_csv(input_linda_tsv_filename, sep)
    d = df[["storage_site", "xml_object_id"]]
    e = create_empty_column("something", 100)
    size = len(df["storage_site"])
    final_df = [ df["storage_site"] ,
                 create_empty_column("file_id", size) ,
                 df["xml_object_id"] ,
                 create_empty_column("file_format", size) ,
                 create_empty_column("file_name", size) ,
                 create_empty_column("file_size", size) ,
                 create_empty_column("md5_sum", size) ,
                 create_empty_column("index_object_id", size) ,
                 create_empty_column("donor_id/donor_count", size),
                 create_empty_column("project_id/project_count", size),
                 create_empty_column("study", size)]
    return pd.concat(final_df, axis=1, sort=False)

def write(df, output_score_manifest_filename, sep='\t'):
    output_dir = os.path.dirname(output_score_manifest_filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    pd.DataFrame.to_csv(df, output_score_manifest_filename, sep, index=False)
    print("Successfully wrote to "+os.path.realpath(output_score_manifest_filename))

def partition_by_size(df, size_per_partition):
    out = []
    start = 0
    end = size_per_partition - 1
    total = len(df)

    final = False
    while(True):
        out.append(df.iloc[start:end+1])
        start += size_per_partition
        end_next = end + size_per_partition + 1
        if final:
            break
        elif end_next > total-1:
            end = total+1
            final = True
        else:
            end = end_next - 1
    return out

def partition_by_number(df, number_of_partitions):
    partition_size = math.floor(len(df)/number_of_partitions)
    return partition_by_size(df, partition_size)


def run(input_linda_tsv_filename, output_score_manifest_filename, sep='\t'):
    df = convert(input_linda_tsv_filename, sep)
    df_partitions = partition_by_number(df, 5)
    count = 0
    for df_partition in df_partitions:
        write(df_partition, output_score_manifest_filename+"."+str(count), sep)
        count += 1

def generate_score_manifests(input_tsv_filename, output_score_manifest_dir, number_of_partitions, sep='\t'):
    df = convert(input_tsv_filename, sep)
    df_partitions = partition_by_number(df, number_of_partitions)
    count = 0
    for df_partition in df_partitions:
        output_filename = "{}/manifest.{}.txt".format(output_score_manifest_dir, str(count))
        write(df_partition, output_filename, sep)
        count += 1
    print("Done")


def create_empty_column(name, size):
    return pd.DataFrame({name: [ None for x in range(0, size)] })

def main():
    generate_score_manifests("/home/rtisma/Documents/oicr/score/create_download_manifest/info_for_collab_song_entries.tsv",
        "./hey/this2", 10 )

if __name__ == '__main__':
    main()

