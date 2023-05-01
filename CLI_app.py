from src.pipeline import KG_builder_pipeline
import click
import ast


class PythonLiteralOption(click.Option):
    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)



@click.command()
# input format: --components '["val1", "val2", ..]'
@click.option("--components", cls=PythonLiteralOption, default='[]')

def main(components):
    if components:
        KG_builder_pipeline.pipeline(components)
    else:
        print("command to execute pipeline: docker compose run --rm cli_app --components '[\"kg_builder\"]'")


if __name__ == "__main__":
    main()