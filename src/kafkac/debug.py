# DEBUG_OPTS allow fine-grained control of librdkafka debugging logs to be
# emitted to the kafkac logger based on explicit values in the KAFKAC_DEBUG
# environ variable value.
DEBUG_OPTS = {
    "cgrp",
    "fetch",
    "topic",
    "consumer",
}


def parse_debug_options(comma_separated_opts: str) -> str:
    """parse_debug_options attempts to read a
    comma separated string of consumer debug options from the
    run time environment.

    For more context on these options, see the `debug` key at:
    https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

    :returns: A comma separated string of consumer debug options.
    """
    provided = {option.lower().strip() for option in comma_separated_opts.split(",")}
    common = provided & DEBUG_OPTS
    return ",".join(common)
