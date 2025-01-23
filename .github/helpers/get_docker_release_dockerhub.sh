#!/bin/bash

# Function to get the next version based on the input parameters
get_next_version_from_dockerhub() {
    local release_branch=$1
    local deployment_type=$2
    local last_release
    local next_version

    tag_pattern="$release_branch"

    all_images=$(docker images "featureformcom/api-server:$tag_pattern.*" --format "{{.Repository}}:{{.Tag}}")
    echo "$all_images"
    image_count="$(echo "$all_images" | wc -l)"
    echo "image count $image_count"

    if [ $image_count == 0 ]; then
        echo "WIP"

    else

        # Separate images with and without -rc
        prerelease_images=()
        release_images=()

        while read -r image; do
            if [[ $image == *"-rc"* ]]; then
                prerelease_images+=("$image")
            else
                release_images+=("$image")
            fi
        done <<< "$all_images"

        # Get the tag of the latest image with -rc
        latest_prerelease=$(printf "%s\n" "${prerelease_images[@]}" | sort -Vr | head -n 1)
        latest_prerelease_tag=$(echo "$latest_prerelease" | awk -F: '{print $2}')

        # Get the tag of the latest image without -rc
        latest_release=$(printf "%s\n" "${release_images[@]}" | sort -Vr | head -n 1)
        latest_release_tag=$(echo "$latest_release" | awk -F: '{print $2}')

        echo "Latest tag with -rc: $latest_prerelease_tag"
        echo
        echo "Latest tag without -rc: $latest_release_tag"

    fi

    ##### Check if it is release or pre release

    # echo $(docker images "featureformcom/api-server:0.0*")
    # Specify the image prefix and tag pattern

    # if [ "$deployment_type" == "pre-release" ]; then
    #     all_images=$(docker images "featureformcom/api-server:$tag_pattern.*-rc")
    #     echo "$all_images"
    #     image_count="$(echo $all_images | wc -l)"

    #     # Subtract 1 to exclude the header line (REPOSITORY, TAG, IMAGE ID, CREATED, SIZE)
    #     image_count=$((image_count - 1))

    #     if [ "$image_count" == 0 ]; then
    #         echo "No previous images"

    #     else

    #         last_row=$(echo "$all_images" | tail -n 1)
    #         last_tag=$(echo "$last_row" | awk '{print $2}')

    #         echo "Image Count: $image_count"
    #         echo "Last Row: $last_row"
    #         echo "Last Tag: $last_tag"

    #     fi




    # elif [ "$deployment_type" == "release" ]; then
    #     all_images=$(docker images "featureformcom/api-server:$tag_pattern.*")
    #     image_count="$(echo $all_images | wc -l)"

    #     # Subtract 1 to exclude the header line (REPOSITORY, TAG, IMAGE ID, CREATED, SIZE)
    #     image_count=$((image_count - 1))

    #     if [ "$image_count" == 0 ]; then
    #         echo "No previous images"

    #     else

    #         last_row=$(echo "$all_images" | tail -n 1)
    #         last_tag=$(echo "$last_row" | awk '{print $2}')

    #         echo "Image Count: $image_count"
    #         echo "Last Row: $last_row"
    #         echo "Last Tag: $last_tag"

    #     fi


    
    # else
    #     echo "Invalid deployment type. Please specify 'pre-release' or 'release'."
    #     exit 1
    # fi 



    ##### Check if there are any docker images with that prefix. Get the last release number accordingly

    # Check if any images match the specified prefix and tag pattern
    # if docker images "featureformcom/api-server:$tag_pattern.*" > /dev/null 2>&1; then

    

    if [ "$image_count" == 0 ]; then
        last_release="0.0.0"
        echo "No images found with the specified prefix and tag pattern."
        
    else
        # Get the latest image
        last_release=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^featureformcom/api-server" | grep "$tag_pattern." | sort -V | tail -n 1)
        echo "Latest image: $last_release"
        
    fi


    ##### Extract the values of major, minor, patches

    major=$(echo "$last_release" | cut -d '.' -f 1)
    minor=$(echo "$last_release" | cut -d '.' -f 2)
    patch=$(echo "$last_release" | cut -d '.' -f 3 | cut -d '-' -f 1)
    rc=$(echo "$last_release" | cut -d '.' -f 3 | cut -d '-' -f 2)


    ##### Pre release: math to get the next version

    # Determine next version based on deployment type
    if [ "$deployment_type" == "pre-release" ]; then
        if [ -z "$rc" ]; then
            # No previous pre-release, start with rc0
            next_version="$major.$minor.$((patch + 1))-rc0"
        else
            # Increment the release candidate
            next_version="$major.$minor.$patch-rc$((rc + 1))"
        fi


    ##### Release: math to get the next version

    elif [ "$deployment_type" == "release" ]; then
        if [ "$last_release" == "" ]; then
            next_version="$release_branch.0"
        else
            # Increment the patch version for the release
            next_version="$major.$minor.$((patch + 1))"
        fi
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
get_next_version_from_dockerhub "$release_branch" "$deployment_type"
