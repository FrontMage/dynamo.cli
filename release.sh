#/bin/bash
echo 'Please input release version'
read -p '>' new_version
echo 'Please input release message'
read -p '>' new_message
echo 'Tagging current version as v'$new_version
git tag -a v$new_version -m "$new_message"
echo 'Pushing tag...'
git push origin v$new_version
echo 'Releasing...'
goreleaser --rm-dist
