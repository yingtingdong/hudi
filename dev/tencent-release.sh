#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e  # Exit immediately if a command exits with a non-zero status

if [ $# -ne 7 ]; then
  echo "Usage: $0 <apache-version> <tencent-version> <rc-num> <release-repo-not-snapshot?> <scala_version> <spark_version> <flink_version>"
  echo "example: $0 0.12.0 1 1 N 2.11 2 1.13"
  exit
fi

version=$1-$2-tencent  # <apache-version>-<tencent-version>-tencent, e.g. 0.10.0-1-tencent
if [ $4 = "N" ]; then
  version=$version-SNAPSHOT
fi
rc=$3
release_repo=$4  # Y for release repo, others for snapshot repo

tag=apache-hudi-$version
tagrc=${tag}-rc${rc}

echo "Preparing source for $tagrc"

# change version
echo "Change version for ${version}"
mvn versions:set -DnewVersion=${version} -DgenerateBackupPom=false -s dev/settings.xml -U
mvn -N versions:update-child-modules
mvn versions:commit -s dev/settings.xml -U

# create version.txt for this release
if [ ${release_repo} = "Y" ]; then
  git add .

  if [ $# -eq 7 ]; then
    git commit -m "Add version tag for release ${version} $5 $6"
  else
    git commit -m "Add version tag for release ${version}"
  fi
else
  git add .

  if [ $# -eq 7 ]; then
    git commit -m"Add snapshot tag ${version} $5 $6"
  else
    git commit -m"Add snapshot tag ${version}"
  fi
fi

set_version_hash=`git rev-list HEAD 2> /dev/null | head -n 1 `

# delete remote tag
git fetch --tags --all
tag_exist=`git tag -l ${tagrc} | wc -l`
if [ ${tag_exist} -gt 0 ]; then
  git tag -l ${tagrc} | xargs git tag -d
  git push origin :refs/tags/${tagrc}
fi

# add remote tag
git tag -am "Apache Hudi $version" ${tagrc} ${set_version_hash}
remote=$(git remote -v | grep data-lake-technology/hudi.git | head -n 1 | awk '{print $1}')
git push ${remote} ${tagrc}

release_hash=`git rev-list ${tagrc} 2> /dev/null | head -n 1 `

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown git tag: $tag"
  exit
fi

echo -e "Using commit ${release_hash}\n"

#echo "git push origin"
#git push origin

echo -e "begin archive ${release_hash}\n"
rm -rf ${tag}*
tarball=$tag.tar.gz

# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
git archive $release_hash --worktree-attributes --prefix $tag/ -o $tarball

# checksum
sha512sum $tarball > ${tarball}.sha512

# extract source tarball
tar xzf ${tarball}

cd ${tag}
if [ ${release_repo} = "N" ]; then
  echo $version > version.txt
fi

echo -e "end archive ${release_hash}\n"

function deploy_spark(){
  echo -------------------------------------------------------
  SCALA_VERSION=$1
  SPARK_VERSION=$2
  FLINK_VERSION=$3

  if [ ${release_repo} = "Y" ]; then
    COMMON_OPTIONS="-Dscala-${SCALA_VERSION} -Dspark${SPARK_VERSION} -Dflink${FLINK_VERSION} -DskipTests -Dcheckstyle.skip=true -Dscalastyle.skip=true -s dev/settings.xml -DretryFailedDeploymentCount=30 -T 2.5C"
  else
    COMMON_OPTIONS="-Dscala-${SCALA_VERSION} -Dspark${SPARK_VERSION} -Dflink${FLINK_VERSION} -DskipTests -Dcheckstyle.skip=true -Dscalastyle.skip=true -s dev/settings.xml -DretryFailedDeploymentCount=30 -T 2.5C"
  fi

#  INSTALL_OPTIONS="-U -Drat.skip=true -Djacoco.skip=true -Dscala-${SCALA_VERSION} -Dspark${SPARK_VERSION} -DskipTests -s dev/settings.xml -T 2.5C"
#
#  echo "INSTALL_OPTIONS: mvn clean package ${INSTALL_OPTIONS}"
#  mvn clean package ${INSTALL_OPTIONS}

  echo "DEPLOY_OPTIONS: mvn clean deploy $COMMON_OPTIONS"
  mvn deploy $COMMON_OPTIONS

  if [ ${release_repo} = "Y" ]; then
    echo -e "Published to release repo\n"
  else
    echo -e "Published to snapshot repo\n"
  fi
  echo -------------------------------------------------------
}

echo "SCALA_VERSION: $5 SPARK_VERSION: $6"
deploy_spark $5 $6 $7

## spark 2.4.6
#deploy_spark 2.11 2
## spark 3.0.1
#deploy_spark 2.12 3.0.x
## spark 3.1.2
#deploy_spark 2.12 3

# clean
rm -rf ../${tag}*

echo "Success! The release candidate [${tagrc}] is available"
echo "Commit SHA1: ${release_hash}"
