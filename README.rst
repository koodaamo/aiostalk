aiostalk
==========

aiostalk is a small and shameless Python client library for communicating
with the `beanstalkd`_ work queue.

It is based on (and requires) another library called `greenstalk`_ by Justin Mayhew.


Getting Started
---------------

Presuming beanstalkd running on localhost at standard port.

.. code-block:: pycon

    >>> import asyncio
    >>> import aiostalk
    >>> 
    >>> async def main():
    ...    client = aiostalk.Client(('127.0.0.1', 11300))
    ...    await client.connect()
    ...    job_id = await client.put('hello')
    ...    print(job_id)
    ...    job = await client.reserve()
    ...    print(job.id)
    ...    print(job.body)
    ...    await client.delete(job)
    ...    await client.close()
    >>> 
    >>> asyncio.run(main())
    1
    1
    hello
    
Using the Client as an asyncio context manager is also supported.


Documentation
-------------

Please see greenstalk docs at `Read the Docs`_.

.. _`beanstalkd`: https://beanstalkd.github.io/
.. _`greenstalk`: https://github.com/justinmayhew/greenstalk
.. _`protocol`: https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt
.. _`Read the Docs`: https://greenstalk.readthedocs.io/
