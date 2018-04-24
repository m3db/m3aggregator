m3x_package               := github.com/m3db/m3x
m3x_package_path          := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver       := 29bc232d9ad2e6c4a1804ccce095bb730fb1c6bc
m3aggregator_package_path := $(gopath_prefix)/$(package_root)

.PHONY: install-m3x-repo
install-m3x-repo: install-glide install-generics-bin
	# Check if repository exists, if not get it
	test -d $(m3x_package_path) || go get -u $(m3x_package)
	test -d $(m3x_package_path)/vendor || (cd $(m3x_package_path) && glide install)
	test "$(shell cd $(m3x_package_path) && git diff --shortstat 2>/dev/null)" = "" || ( \
		echo "WARNING: m3x repository is dirty, generated files might not be as expected" \
	)
	# If does exist but not at min version then update it
	(cd $(m3x_package_path) && git cat-file -t $(m3x_package_min_ver) > /dev/null) || ( \
		echo "WARNING: m3x repository is below commit $(m3x_package_min_ver), generated files might not be as expected" \
	)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-genny-all
test-genny-all: test-genny-map-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all: genny-map-aggregator-entry

# Tests that all currently generated maps match their contents if they were regenerated
.PHONY: test-genny-map-all
test-genny-map-all: genny-map-all
	@test "$(shell git diff --shortstat 2>/dev/null)" = "" || (git diff --no-color && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --porcelain 2>/dev/null | grep "^??")" = "" || (git status --porcelain && echo "Check git status, there are untracked files" && exit 1)

# Map generation rule for aggregator/entryMap
.PHONY: genny-map-aggregator-entry
genny-map-aggregator-entry: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen \
		pkg=aggregator \
		key_type=entryKey \
		value_type=elementPtr \
		out_dir=$(m3aggregator_package_path)/aggregator \
		rename_type_prefix=entry
	# Rename both generated map and constructor files
	mv -f $(m3aggregator_package_path)/aggregator/map_gen.go $(m3aggregator_package_path)/aggregator/entry_map_gen.go
