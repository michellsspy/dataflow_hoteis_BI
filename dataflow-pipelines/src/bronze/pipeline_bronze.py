import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from src.common.logging_utils import get_logger

logger = get_logger("bronze")

class BronzeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, p):
        p.add_argument("--input_url", required=True)
        p.add_argument("--output_path", required=True)

def run():
    opts = BronzeOptions(save_main_session=True, streaming=False)
    with beam.Pipeline(options=opts) as p:
        _ = (p
             | "Read" >> beam.io.ReadFromText(opts.view_as(BronzeOptions).input_url)
             | "Clean" >> beam.Map(lambda x: x.strip())
             | "Write" >> beam.io.WriteToText(opts.view_as(BronzeOptions).output_path, file_name_suffix=".json"))
    logger.info("Bronze finalizado.")

if __name__ == "__main__":
    run()
