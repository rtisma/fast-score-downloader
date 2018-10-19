
# Input TSV format
# storage_site     project_code    song_analysis_id        xml_object_id   xml_file_name   xml_file_size   correct_xml_md5sum      song_xml_md5sum

#Score output TSV format (for manifest
#repo_code       file_id object_id       file_format     file_name       file_size       md5_sum index_object_id donor_id/donor_count    project_id/project_count        study


import pandas as pd
import os
import math
import requests
from datetime import datetime
import concurrent.futures


class ManifestGenerator:
    def __init__(self, input_tsv_filename, output_dir, sep='\t'):
        self.__input_tsv_filename = input_tsv_filename
        self.__output_dir = output_dir
        self.__sep = sep

    def convert_by_partition_num(self, number_of_partitions):
        df = self.__convert_to_manifest()
        return self.__partition_by_number(df, number_of_partitions)

    def convert_by_partition_size(self, partition_size):
        df = self.__convert_to_manifest()
        return self.__partition_by_size(df, partition_size)

    def write_by_partition_num(self, number_of_partitions):
        self.__write_partitions(self.convert_by_partition_num(number_of_partitions))

    def write_by_partition_size(self, partition_size):
        self.__write_partitions(self.convert_by_partition_size(partition_size))

    def __write_partitions(self, df_partitions):
        count = 0
        for df_partition in df_partitions:
            output_filename = "{}/manifest.{}.txt".format(self.__output_dir, str(count))
            self.__write(df_partition, output_filename, self.__sep)
            count += 1
        print("Done")

    def __write(df, output_score_manifest_filename, sep='\t'):
        output_dir = os.path.dirname(output_score_manifest_filename)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        pd.DataFrame.to_csv(df, output_score_manifest_filename, sep, index=False)
        print("Successfully wrote to "+os.path.realpath(output_score_manifest_filename))

    def __convert_to_manifest (self):
        df = pd.read_csv(self.__input_tsv_filename, self.__sep)
        d = df[["storage_site", "xml_object_id"]]
        size = len(df["storage_site"])
        final_df = [ df["storage_site"] ,
                     self.__create_empty_column("file_id", size) ,
                     df["xml_object_id"] ,
                     self.__create_empty_column("file_format", size) ,
                     self.__create_empty_column("file_name", size) ,
                     self.__create_empty_column("file_size", size) ,
                     self.__create_empty_column("md5_sum", size) ,
                     self.__create_empty_column("index_object_id", size) ,
                     self.__create_empty_column("donor_id/donor_count", size),
                     self.__create_empty_column("project_id/project_count", size),
                     self.__create_empty_column("study", size)]
        return pd.concat(final_df, axis=1, sort=False)

    def __create_empty_column(self, name, size):
        return pd.DataFrame({name: [ None for x in range(0, size)] })

    def __partition_by_number(self, df, number_of_partitions):
        partition_size = math.floor(len(df)/number_of_partitions)
        return self.__partition_by_size(df, partition_size)

    def __partition_by_size(self, df, size_per_partition):
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

class StorageDownloader:
    def __init__(self, storage_server_url, access_token, output_dir):
        self.__access_token = access_token
        self.__storage_server_url = storage_server_url
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.__output_dir = os.path.realpath(output_dir)
        self.__headers = { 'Authorization' : 'Bearer '+access_token}

    def download_from_dataframe(self, df):
        if df is None or not isinstance(df, pd.DataFrame):
            raise Exception("the input arg is not an dataframe")
        return self.download(df['xml_object_id'])

    def download(self, object_ids):
        if object_ids is not None or len(object_ids) > 0:
            return [ self.__direct_download(object_id) for object_id in object_ids]
        else:
            return []

    def is_exist(self, object_id):
        return os.path.exists(self.__get_output_filename(object_id))

    def __direct_download(self, object_id):
        output_filename = self.__get_output_filename(object_id)
        resp = requests.get(self.__gen_url(object_id), headers=self.__headers, allow_redirects=True)
        download_url = resp.json()['parts'][0]['url']
        r = requests.get(download_url)
        with open(output_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        return output_filename

    def __get_output_filename(self, object_id):
        return "{}/{}".format(self.__output_dir, object_id)

    def __gen_url(self, object_id):
        return "{}/download/{}?offset=0&length=-1&external=true".format(self.__storage_server_url, object_id)

class BatchJob:
    def __init__(self, storage_downloader, df):
        self.__storage_downloader = storage_downloader
        self.__df = df

    def download_batch(self):
        object_ids = []
        for object_id in self.__df['xml_object_id']:
            if not self.__storage_downloader.is_exist(object_id):
                object_ids.append(object_id)
        return self.__storage_downloader.download(object_ids)

class BatchDownloader:

    def __init__(self, manifest_generator, storage_downloader):
        self.__manifest_generator = manifest_generator
        self.__storage_downloader = storage_downloader

    def download_all(self, num_partitions):
        df_list = self.__manifest_generator.convert_by_partition_num(num_partitions)
        jobs = [BatchJob(self.__storage_downloader, df) for df in df_list]

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_partitions) as executor:
            # Start the load operations and mark each future with its URL
            future_to_batch = {executor.submit(batch_job.download_batch): batch_job for batch_job in jobs}
            for future in concurrent.futures.as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    data = future.result()
                    print("finished {} files".format(str(len(data))))
                except Exception as exc:
                    print('%r generated an exception: %s' % (batch, exc))
                else:
                    print('%r page is %d bytes' % (batch, len(data)))

def main():
    # Config
    input_tsv_filename = "./info_for_collab_song_entries.tsv"
    output_dir = "./output_dir"
    access_token = "your-token-here"
    storage_server_url = "https://storage.cancercollaboratory.org";
    num_threads = 10

    # Buold
    manifest_generator = ManifestGenerator(input_tsv_filename, output_dir)
    storage_downloader = StorageDownloader(storage_server_url, access_token, output_dir)
    batch_downloader = BatchDownloader(manifest_generator, storage_downloader)

    # Execute
    batch_downloader.download_all(num_threads)
    print("DONE")

if __name__ == '__main__':
    main()

