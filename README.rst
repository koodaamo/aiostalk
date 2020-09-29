aiostalk
==========

aiostalk is a small and shameless Python client library for communicating
with the `beanstalkd`_ work queue.

It is based on another library called `greenstalk`_.


Getting Started
---------------

.. code-block:: pycon

    >>> import asyncio
    >>> import aiostalk
    >>> 
    >>> async def main():
    ...    client = aiostalk.Client(('127.0.0.1', 11300))
    ...    await clienct.connect()
    ...    job = await client.put('hello')
    ...    print(job.id)
    ...    job = await client.reserve()
    ...    print(job.id)
    ...    print(job.body)
    ...    await client.delete(job)
    ...    await client.close()
    >>> 
    >>> asyncio.run(main())
    ... 1
    ... 'hello'


Documentation
-------------

Please see greenstalk docs at `Read the Docs`_.

.. _`beanstalkd`: https://beanstalkd.github.io/
.. _`greenstalk`: https://github.com/justinmayhew/greenstalk
.. _`protocol`: https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt
.. _`Read the Docs`: https://greenstalk.readthedocs.io/
