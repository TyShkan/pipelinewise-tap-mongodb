import copy
import time
import datetime
import singer

from typing import Set, Dict, Optional, Generator
from pymongo.collection import Collection
from pymongo.database import Database
from singer import utils

from tap_mongodb.sync_strategies import common
from tap_mongodb.metrics import record_counter_dynamic

LOGGER = singer.get_logger('tap_mongodb')

RESUME_TOKEN_KEY = 'token'
DEFAULT_AWAIT_TIME_MS = 1000  # the server default https://docs.mongodb.com/manual/reference/method/db.watch/#db.watch


def update_bookmarks(state: Dict, tap_stream_ids: Set[str], token: Dict) -> Dict:
    """
    Updates the stream state by re-setting the changeStream token
    Args:
        state: State dictionary
        tap_stream_ids: set of streams' ID
        token: resume token from changeStream

    Returns:
        state: updated state
    """
    for stream in tap_stream_ids:
        state = singer.write_bookmark(state, stream, RESUME_TOKEN_KEY, token)

    return state


def get_token_from_state(streams_to_sync: Set[str], state: Dict) -> Optional[Dict]:
    """
    Extract the smallest non null resume token
    Args:
        streams_to_sync: set of log based streams
        state: state dictionary

    Returns: resume token if found, None otherwise

    """
    token_sorted = sorted([stream_state[RESUME_TOKEN_KEY]
                           for stream_name, stream_state in state.get('bookmarks', {}).items()
                           if stream_name in streams_to_sync and stream_state.get(RESUME_TOKEN_KEY) is not None],
                          key=lambda key: key['_data'])

    return token_sorted[0] if token_sorted else None


def sync_database(database: Database,
                  streams_to_sync: Dict[str, Dict],
                  state: Dict,
                  await_time_ms: int,
                  start_at_operation_time: Optional[datetime.datetime]
                  ) -> None:
    """
    Syncs the records from the given collection using ChangeStreams
    Args:
        database: MongoDB Database instance to sync
        streams_to_sync: Dict of stream dictionary with all the stream details
        state: state dictionary
        await_time_ms:  the maximum time in milliseconds for the log based to wait for changes before exiting
    """
    LOGGER.info('Starting LogBased sync for streams "%s" in database "%s"', list(streams_to_sync.keys()), database.name)

    rows_saved = {}
    start_time = time.time()

    for stream_id in streams_to_sync:
        rows_saved[stream_id] = 0

    stream_ids = set(streams_to_sync.keys())

    token = get_token_from_state(stream_ids, state)

    if token:
        start_at = None
    else:
        start_at = common.get_bson_timestamp_from_datetime(start_at_operation_time)

    LOGGER.info('Starting WATCH with token %s, start_at %s', token, start_at)

    with record_counter_dynamic() as counter:
        # Init a cursor to listen for changes from the last saved resume token
        # if there are no changes after MAX_AWAIT_TIME_MS, then we'll exit
        with database.watch(
                [{'$match': {
                    '$or': [
                        {'operationType': 'insert'}, {'operationType': 'update'}, {'operationType': 'delete'}, {'operationType': 'replace'}
                    ],
                    '$and': [
                        # watch collections of selected streams
                        {'ns.coll': {'$in': [val['table_name'] for val in streams_to_sync.values()]}}
                    ]
                }}],
                max_await_time_ms=await_time_ms,
                start_after=token,
                start_at_operation_time=start_at,
                full_document='updateLookup'
        ) as cursor:
            while cursor.alive:

                change = cursor.try_next()

                # Note that the ChangeStream's resume token may be updated
                # even when no changes are returned.

                # Token can look like in MongoDB 4.2:
                #       {'_data': 'A_LONG_HEX_DECIMAL_STRING'}
                #    or {'_data': 'A_LONG_HEX_DECIMAL_STRING', '_typeBits': b'SOME_HEX'}

                # Get the '_data' only from resume token
                # token can contain a property '_typeBits' of type bytes which cannot be json
                # serialized when creating the state.
                # '_data' is enough to resume LOG_BASED
                resume_token = {
                    '_data': cursor.resume_token['_data']
                }

                # After MAX_AWAIT_TIME_MS has elapsed, the cursor will return None.
                # write state and exit
                if change is None:
                    LOGGER.info('No change streams after %s, updating bookmark and exiting...', await_time_ms)

                    state = update_bookmarks(state, stream_ids, resume_token)
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                    break

                tap_stream_id = f'{change["ns"]["db"]}-{change["ns"]["coll"]}'
                stream_version = common.get_stream_version(tap_stream_id, state)

                operation = change['operationType']

                payload_timestamp = change['clusterTime'].as_datetime() # returns python's datetime.datetime instance in UTC

                if operation == 'insert':
                    singer.write_message(common.row_to_singer_record(stream=streams_to_sync[tap_stream_id],
                                                                    row=change['fullDocument'],
                                                                    time_extracted=utils.now(),
                                                                    time_deleted=None,
                                                                    time_updated=payload_timestamp,
                                                                    version=stream_version))

                    rows_saved[tap_stream_id] += 1
                    counter.increment(endpoint=tap_stream_id, metric_type="inserted")

                elif operation == 'replace':
                    singer.write_message(common.row_to_singer_record(stream=streams_to_sync[tap_stream_id],
                                                                    row=change['fullDocument'],
                                                                    time_extracted=utils.now(),
                                                                    time_deleted=None,
                                                                    time_updated=payload_timestamp,
                                                                    version=stream_version))

                    rows_saved[tap_stream_id] += 1
                    counter.increment(endpoint=tap_stream_id, metric_type="replaced")

                elif operation == 'update':
                    full_document = change.get('fullDocument')

                    if full_document:
                        singer.write_message(common.row_to_singer_record(stream=streams_to_sync[tap_stream_id],
                                                                        row=full_document,
                                                                        time_extracted=utils.now(),
                                                                        time_deleted=None,
                                                                        time_updated=payload_timestamp,
                                                                        version=stream_version))

                        rows_saved[tap_stream_id] += 1
                        counter.increment(endpoint=tap_stream_id, metric_type="updated")
                    else:
                        LOGGER.warning(f"Received UPDATE operation for already deleted object, skipping update: {change}")
                        counter.increment(endpoint=tap_stream_id, metric_type="skipped")

                elif operation == 'delete':
                    # Delete ops only contain the _id of the row deleted
                    singer.write_message(common.row_to_singer_record(
                        stream=streams_to_sync[tap_stream_id],
                        row={'_id': change['documentKey']['_id']},
                        time_extracted=utils.now(),
                        time_deleted=payload_timestamp,
                        version=stream_version))

                    rows_saved[tap_stream_id] += 1
                    counter.increment(endpoint=tap_stream_id, metric_type="deleted")

                counter.increment(endpoint=tap_stream_id)

                # update the states of all streams
                state = update_bookmarks(state, stream_ids, resume_token)

                # write state every UPDATE_BOOKMARK_PERIOD messages
                if sum(rows_saved.values()) % common.UPDATE_BOOKMARK_PERIOD == 0:
                    LOGGER.debug('Writing state...')
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    for stream_id in stream_ids:
        common.COUNTS[stream_id] += rows_saved[stream_id]
        common.TIMES[stream_id] += time.time() - start_time
        LOGGER.info('Synced %s records for %s', rows_saved[stream_id], stream_id)
