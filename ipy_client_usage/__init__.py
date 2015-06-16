from datetime import datetime
import json
import shutil
import time


def json_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    else:
        raise TypeError("Type {} not serializable".format(type(obj)))


def dump(fname, client, uuid_to_name=None):
    """Dump the data to the file fname and return the time this takes."""
    # Write to a temporary file, then move it.  This makes the
    # complete file available more often in the case that there is a
    # lot of data and dumping takes a while.
    tic = time.time()
    temp_name = fname + '_tmp'
    with open(temp_name, 'w') as fh:
        uuid_to_name = uuid_to_name or {}

        ids = client.history
        result = client.get_result(ids)
        names = [uuid_to_name.get(job_id, job_id) for job_id in ids]
        metadata = dict(zip(ids, zip(names, result.metadata)))
        json.dump(metadata, fh, default=json_datetime)

    shutil.move(temp_name, fname)
    return time.time() - tic


def wait_and_dump(fname, client, sample_frequency=0.5, uuid_to_name=None,
                  timeout=-1):
    # A close lift from Client.wait.
    tic = time.time()
    theids = client.outstanding
    dump_time = dump(fname, client, uuid_to_name=uuid_to_name)
    last_time = time.time()
    while theids.intersection(client.outstanding):
        if timeout >= 0 and (time.time() - tic) > timeout:
            break
        time_since_last_dump = time.time() - last_time
        time.sleep(max(sample_frequency, 2*dump_time) - time_since_last_dump)
        client.spin()
        dump_time = dump(fname, client, uuid_to_name=uuid_to_name)
    return len(theids.intersection(client.outstanding)) == 0
