"""Exception diagnostics must not execute more application code."""
import json
import unittest

from orch8_engine import PermanentError, _wrap


class HandlerErrorsTest(unittest.TestCase):
    def reply(self, error):
        def host(_):
            raise error
        try:
            result = _wrap(host)('{}')
        except BaseException:
            # Do not ask unittest to format the hostile exception itself.
            return {'normalization_failed': True}
        return json.loads(result)['error']

    def test_exception_hooks_are_not_called(self):
        calls = []
        class Hostile(Exception):
            @property
            def permanent(self):
                calls.append('permanent')
                raise RuntimeError('property hook')
            def __str__(self):
                calls.append('str')
                raise RuntimeError('string hook')
        self.assertEqual(self.reply(Hostile('original')), {'message': 'original', 'permanent': False})
        self.assertEqual(calls, [])

    def test_attribute_override_is_not_called(self):
        calls = []
        class Hostile(Exception):
            def __getattribute__(self, name):
                calls.append(name)
                raise RuntimeError('attribute hook')
        self.assertEqual(self.reply(Hostile('original')), {'message': 'original', 'permanent': False})
        self.assertEqual(calls, [])

    def test_permanent_subclass_preserves_terminal_policy_without_formatting(self):
        class Terminal(PermanentError):
            def __str__(self):
                raise RuntimeError('string hook')
        self.assertEqual(self.reply(Terminal('stop')), {'message': 'stop', 'permanent': True})
        marked = Exception('marked')
        marked.permanent = True
        self.assertTrue(self.reply(marked)['permanent'])

    def test_opaque_arguments_do_not_get_formatted(self):
        calls = []
        class Argument:
            def __str__(self):
                calls.append('str')
                raise RuntimeError('argument hook')
            def __repr__(self):
                calls.append('repr')
                raise RuntimeError('argument hook')
        self.assertEqual(self.reply(Exception(Argument())), {'message': 'handler failed', 'permanent': False})
        self.assertEqual(calls, [])

    def test_messages_are_bounded_and_native_decodable(self):
        result = self.reply(Exception('x' * 2048))
        self.assertEqual(result['message'], 'x' * 1024)
        result = self.reply(Exception('bad\ud800text'))
        result['message'].encode('utf-8', errors='strict')

    def test_serialization_exception_is_normalized(self):
        class Broken(dict):
            def items(self):
                class Problem(Exception):
                    def __str__(self):
                        raise RuntimeError('secondary failure')
                raise Problem('serialization failed')
        reply = json.loads(_wrap(lambda _: Broken(a=1))('{}'))
        self.assertEqual(reply, {'error': {'message': 'serialization failed', 'permanent': False}})

    def test_colliding_attribute_keys_do_not_run_equality(self):
        calls = []
        class Key:
            def __hash__(self):
                return hash('permanent')
            def __eq__(self, other):
                calls.append('eq')
                raise RuntimeError('key hook')
        error = Exception('safe')
        error.__dict__[Key()] = True
        self.assertEqual(self.reply(error), {'message': 'safe', 'permanent': False})
        self.assertEqual(calls, [])

    def test_attribute_inspection_is_bounded(self):
        error = Exception('safe')
        error.__dict__.update({f'field{i}': i for i in range(65)})
        error.permanent = True
        self.assertFalse(self.reply(error)['permanent'])
        terminal = PermanentError('stop')
        terminal.__dict__.update(error.__dict__)
        self.assertTrue(self.reply(terminal)['permanent'])

    def test_native_retry_and_terminal_policy_survive_hostile_formatting(self):
        from orch8_engine import Engine
        calls = []
        hooks = []
        class Retry(Exception):
            @property
            def permanent(self):
                hooks.append('permanent')
                raise RuntimeError('property hook')
            def __str__(self):
                hooks.append('str')
                raise RuntimeError('string hook')
        class Stop(PermanentError):
            def __str__(self):
                hooks.append('terminal str')
                raise RuntimeError('string hook')
        engine = Engine.open(':memory:')
        def flaky(ctx):
            calls.append(ctx['attempt'])
            if len(calls) == 1:
                raise Retry('temporary')
            return True
        def terminal(_):
            raise Stop('stop')
        engine.handler('flaky', flaky)
        engine.handler('terminal', terminal)
        try:
            retry = {'max_attempts': 3, 'initial_backoff': 10, 'max_backoff': 10}
            for name in ['flaky', 'terminal']:
                engine.deploy({'name': name, 'blocks': [
                    {'type': 'step', 'id': 'work', 'handler': name, 'retry': retry}]})
            self.assertEqual(engine.run(engine.start('flaky'), timeout=5)['state'], 'completed')
            self.assertEqual(calls, [0, 1])
            failed = engine.run(engine.start('terminal'), timeout=5)
            self.assertEqual(failed['state'], 'failed')
            self.assertIn('stop', failed['outputs']['work']['message'])
            self.assertEqual(hooks, [])
        finally:
            engine.close()

    def test_attribute_dictionary_subclass_is_not_called(self):
        calls = []
        class Attributes(dict):
            def items(self):
                calls.append('items')
                raise RuntimeError('dictionary hook')
        error = Exception('safe')
        error.__dict__ = Attributes(permanent=True)
        self.assertEqual(self.reply(error), {'message': 'safe', 'permanent': True})
        self.assertEqual(calls, [])
