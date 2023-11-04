#!/usr/bin/env python3
import copy
import time
import pymongo
import singer

from typing import Dict, Optional
from pymongo.collection import Collection
from singer import metadata, utils

from tap_mongodb.sync_strategies import common

LOGGER = singer.get_logger('tap_mongodb')


def update_bookmark(row: Dict, state: Dict, tap_stream_id: str, replication_key_name: str) -> Dict:
    """
    Updates replication key and type values in state bookmark
    Args:
        row: DB record
        state: dictionary of bookmarks
        tap_stream_id: stream ID
        replication_key_name: replication key
    """
    replication_key_value = row.get(replication_key_name)

    if replication_key_value:
        replication_key_type = replication_key_value.__class__.__name__

        replication_key_value_bookmark = common.class_to_string(replication_key_value, replication_key_type)

        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'replication_key_value',
                                      replication_key_value_bookmark)

        state = singer.write_bookmark(state,
                              tap_stream_id,
                              'replication_key_type',
                              replication_key_type)

    return state


def sync_collection(collection: Collection,
                    stream: Dict,
                    state: Optional[Dict],
                    ) -> None:
    """
    Syncs the stream records incrementally
    Args:
        collection: MongoDB collection instance
        stream: stream dictionary
        state: state dictionary if exists
    """
    LOGGER.info('Starting incremental sync for %s', stream['tap_stream_id'])

    nascent_stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    # before writing the table version to state, check if we had one to begin with
    if nascent_stream_version is None:
        nascent_stream_version = int(time.time() * 1000)
        first_run = True
    else:
        first_run = False

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        activate_version_message = singer.ActivateVersionMessage(
            stream=common.calculate_destination_stream_name(stream),
            version=nascent_stream_version
        )
        LOGGER.info("Activate version %s", nascent_stream_version)
        singer.write_message(activate_version_message)

    # get replication key, and bookmarked value/type
    stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'], {})

    replication_key_name = metadata.to_map(stream['metadata']).get(()).get('replication-key')

    # create query
    find_filter = {}

    if stream_state.get('replication_key_value'):
        find_filter[replication_key_name] = {}
        find_filter[replication_key_name]['$gte'] = common.string_to_class(stream_state.get('replication_key_value'),
                                                                           stream_state.get('replication_key_type'))

    # log query
    LOGGER.info('Querying %s with: %s', stream['tap_stream_id'], dict(find=find_filter))

    with collection.find(find_filter,
                         sort=[(replication_key_name, pymongo.ASCENDING)]) as cursor:
        rows_saved = 0
        start_time = time.time()

        for row in cursor:

            singer.write_message(common.row_to_singer_record(stream=stream,
                                                             row=row,
                                                             time_extracted=utils.now(),
                                                             time_deleted=None,
                                                             version=nascent_stream_version))
            rows_saved += 1

            state = update_bookmark(row, state, stream['tap_stream_id'], replication_key_name)

            if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        common.COUNTS[stream['tap_stream_id']] += rows_saved
        common.TIMES[stream['tap_stream_id']] += time.time() - start_time

    LOGGER.info('Syncd %s records for %s', rows_saved, stream['tap_stream_id'])
