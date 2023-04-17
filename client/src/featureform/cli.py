# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import click
from featureform import ResourceClient
from .list import *
from .get import *
import os
from flask import Flask
from .dashboard_metadata import dashboard_app
import validators
import urllib.request

resource_types = [
    "feature",
    "source",
    "training-set",
    "label",
    "entity",
    "provider",
    "model",
    "user"
]

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    \b
    ______         _                   __                     
    |  ___|       | |                 / _|                    
    | |_ ___  __ _| |_ _   _ _ __ ___| |_ ___  _ __ _ __ ___  
    |  _/ _ \/ _` | __| | | | '__/ _ \  _/ _ \| '__| '_ ` _ \ 
    | ||  __/ (_| | |_| |_| | | |  __/ || (_) | |  | | | | | |
    \_| \___|\__,_|\__|\__,_|_|  \___|_| \___/|_|  |_| |_| |_|

    Interact with Featureform's Feature Store via the official command line interface.
    """
    pass


@cli.command()
@click.option("--host",
              "host",
              required=False,
              help="The host address of the API server to connect to")
@click.option("--cert",
              "cert",
              required=False,
              help="Path to self-signed TLS certificate")
@click.option("--insecure",
              is_flag=True,
              help="Disables TLS verification")
@click.option("--local",
              is_flag=True,
              help="Enables local mode")
@click.argument("resource_type", required=True)
@click.argument("name", required=True)
@click.argument("variant", required=False)
def get(host, cert, insecure, local, resource_type, name, variant):
    """Get resources of a given type.
    """
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")

    rc = ResourceClient(host=host, local=local, insecure=insecure, cert_path=cert)

    rc_get_functions_variant = {
        "feature": rc.print_feature,
        "label": rc.print_label,
        "source": rc.print_source,
        "trainingset": rc.print_training_set,
        "training-set": rc.print_training_set
    }

    rc_get_functions = {
        "user": rc.get_user,
        "model": rc.get_model,
        "entity": rc.get_entity,
        "provider": rc.get_provider
    }

    if resource_type in rc_get_functions_variant:
        rc_get_functions_variant[resource_type](name=name, variant=variant, local=local)
    elif resource_type in rc_get_functions:
        rc_get_functions[resource_type](name=name, local=local)
    else:
        raise ValueError("Resource type not found")


@cli.command()
@click.option("--host",
              "host",
              required=False,
              help="The host address of the API server to connect to")
@click.option("--cert",
              "cert",
              required=False,
              help="Path to self-signed TLS certificate")
@click.option("--insecure",
              is_flag=True,
              help="Disables TLS verification")
@click.option("--local",
              is_flag=True,
              help="Enable local mode")
@click.argument("resource_type", required=True)
def list(host, cert, insecure, local, resource_type):
    if local:
        if host != None:
            raise ValueError("Cannot be local and have a host")

    elif host == None:
        host = os.getenv('FEATUREFORM_HOST')
        if host == None:
            raise ValueError(
                "Host value must be set with --host flag or in env as FEATUREFORM_HOST")

    rc = ResourceClient(host=host, local=local, insecure=insecure, cert_path=cert)

    rc_list_functions = {
        "features": rc.list_features,
        "labels": rc.list_labels,
        "sources": rc.list_sources,
        "trainingsets": rc.list_training_sets,
        "training-sets": rc.list_training_sets,
        "users": rc.list_users,
        "models": rc.list_models,
        "entities": rc.list_entities,
        "providers": rc.list_providers
    }

    if resource_type in rc_list_functions:
        rc_list_functions[resource_type](local=local)
    else:
        raise ValueError("Resource type not found")


app = Flask(__name__)
app.register_blueprint(dashboard_app)


@cli.command()
def dash():
    app.run(threaded=True, port=os.getenv("LOCALMODE_DASHBOARD_PORT", 3000))


@cli.command()
@click.argument("files", required=True, nargs=-1)
@click.option("--host",
              "host",
              required=False,
              help="The host address of the API server to connect to")
@click.option("--cert",
              "cert",
              required=False,
              help="Path to self-signed TLS certificate")
@click.option("--insecure",
              is_flag=True,
              help="Disables TLS verification")
@click.option("--local",
              is_flag=True,
              help="Enable local mode")
@click.option("--dry-run",
              is_flag=True,
              help="Checks the definitions without applying them")
@click.option("--no-wait",
              is_flag=True,
              help="Applies the resources asynchronously")
def apply(host, cert, insecure, local, files, dry_run, no_wait):
    for file in files:
        if os.path.isfile(file):
            read_file(file)
        elif validators.url(file):
            read_url(file)
        else:
            raise ValueError(
                f"Argument must be a path to a file or URL with a valid schema (http:// or https://): {file}")

    rc = ResourceClient(host=host, local=local, insecure=insecure, cert_path=cert, dry_run=dry_run)
    asynchronous = no_wait
    rc.apply(asynchronous=asynchronous)


@cli.command()
@click.option("--query",
              "-q",
              "query",
              required=True,
              help="The phrase to search resources (e.g. 'quick').")
@click.option("--host",
              "host",
              required=False,
              help="The host address of the API server to connect to")
@click.option("--cert",
              "cert",
              required=False,
              help="Path to self-signed TLS certificate")
@click.option("--insecure",
              is_flag=True,
              help="Disables TLS verification")
@click.option("--local",
              is_flag=True,
              help="Enable local mode")
def search(query, host, cert, insecure, local):
    rc = ResourceClient(host=host, local=local, insecure=insecure, cert_path=cert)
    results = rc.search(query, local)
    if local:
        format_rows("NAME", "VARIANT", "TYPE")
        for r in results:
            desc = r["description"][:cutoff_length] + "..." if len(r["description"]) > 0 else ""
            format_rows(r["name"], r["variant"], r["resource_type"])


def read_file(file):
    with open(file, "r") as py:
        exec_file(py, file)


def read_url(url):
    try:
        with urllib.request.urlopen(url) as py:
            exec_file(py, url)
    except Exception as e:
        raise ValueError(f"Could not apply the provided URL: {e}: {url}")


def exec_file(file, name):
    code = compile(file.read(), name, "exec")
    # Create a new global namespace for each file to ensure that
    # global variables, such as `ff`, are not undefined in the
    # context of class attribute assignments (e.g. `label = ff.Label()`)
    file_globals = {}
    exec(code, file_globals)


if __name__ == '__main__':
    cli()
