#!/bin/bash
set -e
set -x

DIR=$(cd `dirname $0` && cd ../../ && pwd)

# Uploads different formats of the manual to a public server.


# Which version the documentation is now.
VERSION=$(cat $DIR/target/src/version)

# Name of the symlink created on the docs server, pointing to this version.
if [[ $VERSION == *SNAPSHOT* ]]
then
	SYMLINKVERSION=snapshot
else
	if [[ $VERSION == *M* ]]
	then
		SYMLINKVERSION=milestone
	else
		SYMLINKVERSION=stable
	fi
fi
DOCS_SERVER='neo@docs.neo4j.org'
ROOTPATHDOCS='/data/www/doc/docs.neo4j.org'
hostname=$(uname -n)

# If you're not a Jenkins node, don't deploy the docs
#[ "${hostname}" == 'build1' ] &&  exit 0

echo "VERSION = $VERSION"
echo "SYMLINKVERSION = $SYMLINKVERSION"

IDENTITY_FILE=${HOME}/.ssh/neo_at_docs_neo4j_org

# Create initial directories
ssh -i $IDENTITY_FILE $DOCS_SERVER mkdir -p $ROOTPATHDOCS/{text,chunked}/$VERSION
#ssh $DOCS_SERVER mkdir -p $ROOTPATHDOCS/{text,chunked,annotated}/$VERSION

# Copy artifacts
rsync -e "ssh -i $IDENTITY_FILE" -r $DIR/target/text/ $DOCS_SERVER:$ROOTPATHDOCS/text/$VERSION/
#rsync -r --delete $DIR/target/annotated/ $DOCS_SERVER:$ROOTPATHDOCS/annotated/
rsync -e "ssh -i $IDENTITY_FILE" -r --delete $DIR/target/chunked/ $DOCS_SERVER:$ROOTPATHDOCS/chunked/$VERSION/
ssh -i $IDENTITY_FILE $DOCS_SERVER mkdir -p $ROOTPATHDOCS/pdf
scp -i $IDENTITY_FILE $DIR/target/pdf/neo4j-manual.pdf $DOCS_SERVER:$ROOTPATHDOCS/pdf/neo4j-manual-$VERSION.pdf

# Symlink this version to a generic url
#ssh $DOCS_SERVER "cd $ROOTPATHDOCS/text/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
#ssh $DOCS_SERVER "cd $ROOTPATHDOCS/chunked/ && (rm $SYMLINKVERSION || true); ln -s $VERSION $SYMLINKVERSION"
#ssh $DOCS_SERVER "cd $ROOTPATHDOCS/pdf/ && (rm neo4j-manual-$SYMLINKVERSION.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-$SYMLINKVERSION.pdf"

#if [[ $SYMLINKVERSION == stable ]]
#then
  #ssh $DOCS_SERVER "cd $ROOTPATHDOCS/text/ && (rm milestone || true); ln -s $VERSION milestone"
  #ssh $DOCS_SERVER "cd $ROOTPATHDOCS/chunked/ && (rm milestone || true); ln -s $VERSION milestone"
  #ssh $DOCS_SERVER "cd $ROOTPATHDOCS/pdf/ && (rm neo4j-manual-milestone.pdf || true); ln -s neo4j-manual-$VERSION.pdf neo4j-manual-milestone.pdf"
#fi


echo Apparently, successfully published to $DOCS_SERVER.


