import logging
import pathlib
from typing import TextIO

import click


class NaturalOrderGroup(click.Group):
    """See https://github.com/pallets/click/issues/513."""
    def list_commands(self, ctx):
        return self.commands.keys()


class EmitterContextManager:
    """Maintain file pointers to output directory."""

    def __init__(self, output_path: str, verbose=False, file_mode="w",
                 logger=logging.getLogger("EmitterContextManager")):
        """Ensure output_path exists, init emitter dict."""
        output_path = pathlib.Path(output_path)
        if not output_path.exists():
            output_path.mkdir(parents=True)
        assert output_path.is_dir(), f"{output_path} not a directory?"

        self.output_path = output_path
        """destination directory"""
        self.emitters = {}
        """open file pointers"""
        self.verbose = verbose
        """log activity"""
        self.file_mode = file_mode
        """mode for file opens"""
        self.logger = logger

    def __enter__(self):
        """Ensure output_path exists, init emitter dict.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Close all open files."""
        for _ in self.emitters.values():
            _.close()
            if self.verbose:
                self.logger.info(f"wrote {_.name}")

    def emit(self, name: str) -> TextIO:
        """Maintain a hash of open files."""
        if name not in self.emitters:
            self.emitters[name] = open(self.output_path / f"{name}.ndjson", self.file_mode)
            if self.verbose:
                self.logger.debug(f"opened {self.emitters[name].name}")
        return self.emitters[name]
