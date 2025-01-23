#!/bin/bash

##### File reading ignores the release number command line argument
# Function to get the next version based on the input parameters
get_next_version_from_file() {
    local release_branch=$1
    local deployment_type=$2
    local last_release
    local last_prerelease
    local next_version

    ##### Check if a file exists, create it if it doesnt, read from it if it does

    file_path="release_version.txt"

    # Check if the file exists
    if [ ! -e "$file_path" ]; then
        # Create the file if it doesn't exist
        touch "$file_path"
        echo "File $file_path created."
    fi

    file_content=$(cat $file_path)


    ##### Read the last release or pre lease version depending on the tag

    # if [ "$deployment_type" == "pre-release" ]; then
        last_prerelease=$( echo "$file_content" | grep "^prerelease=" | cut -d '=' -f 2)

    # elif [ "$deployment_type" == "release" ]; then
        last_release=$( echo "$file_content" | grep "^release=" | cut -d '=' -f 2)
    
    # else
    #     echo "Invalid deployment type. Please specify 'pre-release' or 'release'."
    #     exit 1
    # fi 

    echo "The last release is $last_release"
    echo "The last prerelease is $last_prerelease"


    ##### Extract the values of major, minor, patches

    major_rel=$(echo "$last_release" | cut -d '.' -f 1)
    minor_rel=$(echo "$last_release" | cut -d '.' -f 2)
    patch_rel=$(echo "$last_release" | cut -d '.' -f 3)

    major_prerel=$(echo "$last_prerelease" | cut -d '.' -f 1)
    minor_prerel=$(echo "$last_prerelease" | cut -d '.' -f 2)
    patch_prerel=$(echo "$last_prerelease" | cut -d '.' -f 3 | cut -d '-' -f 1)
    rc=$(echo "$last_prerelease" | cut -d '.' -f 3 | cut -d 'c' -f 2)


    ##### Pre release: math to get the next version

    # Determine next version based on deployment type
    if [ "$deployment_type" == "pre-release" ]; then
        if [ "$last_prerelease" == "" ]; then
            if [ "$last_release" == "" ]; then
                next_version="$release_branch.0-rc0"
            else
                next_version="$major_rel.$minor_rel.$((patch_rel + 1))-rc0"
            fi
        else
            echo "enters here"
            # Increment the release candidate
            next_version="$major_prerel.$minor_prerel.$patch_prerel-rc$((rc + 1))"
        fi
        write_to_file="${file_content/prerelease=$last_prerelease/prerelease=$next_version}"
        echo "$write_to_file" > "$file_path"


    ##### Release: math to get the next version

    elif [ "$deployment_type" == "release" ]; then
        if [ "$last_release" == "" ]; then
            next_version="$release_branch.0"
            # next_prerelease_version=$last_prerelease
        else
            # Increment the patch version for the release
            next_version="$major_rel.$minor_rel.$((patch_rel + 1))"
            # next_prerelease_version="$major_rel.$minor_rel.$((patch_rel + 1))-rc-1"
        fi
        write_to_file="release=$next_version"$'\n'"prerelease="
        # write_to_file="${file_content/release=$last_release/release=$next_version}"
        echo "$write_to_file" > "$file_path"
        # write_pre_to_file="${file_content/prerelease=$last_prerelease/prerelease=$next_prerelease_version}"
        # echo "$write_pre_to_file" > "$file_path"

    else
        echo "Invalid deployment type. Please specify 'pre-release' or 'release'."
        exit 1
    fi

    echo "Next version is $next_version"
}

##### Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--branch)
            release_branch="$2"
            shift 2
            ;;
        -d|--deployment)
            deployment_type="$2"
            shift 2
            ;;
        *)
            echo "Invalid argument: $1"
            exit 1
            ;;
    esac
done

##### Check if required arguments are provided
if [ -z "$release_branch" ] || [ -z "$deployment_type" ]; then
    echo "Usage: $0 -b|--branch <release_branch> -d|--deployment <pre-release|release>"
    exit 1
fi

# Call the function to get the next version
get_next_version_from_file "$release_branch" "$deployment_type"
