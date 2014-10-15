all: uninstall install

uninstall:
	R -e "remove.packages(\"hdps\")"

install:
	R -e "library(devtools); install()"
