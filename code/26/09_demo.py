# What `make demo` shows, run on the build from nothing: the night, the
# proof, one November for every reader, and three refusals.
from _sandbox import fresh

from groundwork import demo

demo.main(["--warehouse", str(fresh())])
