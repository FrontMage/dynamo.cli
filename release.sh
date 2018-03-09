#/bin/bash
echo 'Please input release version'
read -p '>' new_version
echo 'Please input release message'
read -p '>' new_message
echo 'Tagging current version as v'$new_version
exec git tag -a v$new_version -m "$new_message"
echo 'Pushing tag...'
exec git push origin v$new_version
echo 'Relasing...'
exec goreleaser --rm-dist
