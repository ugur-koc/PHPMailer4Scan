<?php

declare(strict_types=1);

namespace spec\Jobcloud\MarketplaceAdapterEnrichment\Config;

use FOO\MarketplaceAdapterEnrichment\Config\ConfigInterface;
use FOO\MarketplaceAdapterEnrichment\Config\KafkaConnectionConfig;
use FOO\MarketplaceAdapterEnrichment\Model\ConfigContextInterface;
use FOO\ParameterManager\Pool\ParameterPoolManagerInterface;
use PhpSpec\ObjectBehavior;
use spec\Jobcloud\MarketplaceAdapterEnrichment\Helper\ProphetTrait;


final class KafkaConnectionConfigSpec extends ObjectBehavior
{
    use ProphetTrait;

    private const KAFKA_BROKER_URL = 'PLAINTEXT://foo:9095';

    /**
     * @type array<string, string>
     */
    private const array REGISTRY_OPTIONS = [
        'url' => 'http://foo:9081',
        'username' => 'root',
        'password' => 'root',
    ];

    private const KAFKA_CERTIFICATES = [
        'kafka_ca' => '-----BEGIN CERTIFICATE----- -----END CERTIFICATE-----',
        'kafka_user_key' => '-----BEGIN PRIVATE KEY----- -----END PRIVATE KEY-----',
        'kafka_user_cert' => '-----BEGIN CERTIFICATE----- -----END CERTIFICATE-----',
    ];

    public function it_is_instance_of_config_interface(): void
    {
        $this->shouldBeAnInstanceOf(ConfigInterface::class);
    }

    public function it_throws_invalid_argument_exception_if_parameter_pool_manager_is_not_set(
        ConfigContextInterface $configContext,
    ): void {
        $exception = new \InvalidArgumentException('ParameterPoolManager must bes set for KafkaConnectionConfig.');

        $this->shouldThrow($exception)->during('getConfig', [$configContext]);
    }

    public function it_creates_config_for_dev(
        ParameterPoolManagerInterface $parameterPoolManager,
        ConfigContextInterface $configContext,
    ): void {
        $this->mockSchemaRegistryParameters($parameterPoolManager);

        $configContext->isDevOrTest()->shouldBeCalledTimes(2)->willReturn(true);

        $this->getConfig($configContext, $parameterPoolManager)->shouldBeEqualTo([
            'kafka.broker.url' => self::KAFKA_BROKER_URL,
            'kafka.schema.registry.options' => [
                'url' => self::REGISTRY_OPTIONS['url'],
            ],
            'kafka.security.options' => [],
        ]);
    }

    public function it_creates_config_for_prod(
        ParameterPoolManagerInterface $parameterPoolManager,
        ConfigContextInterface $configContext,
    ): void {
        $this->mockSchemaRegistryParameters($parameterPoolManager);
        $this->mockKafkaCertificatesParameters($parameterPoolManager);

        $configContext->isDevOrTest()->shouldBeCalledTimes(2)->willReturn(false);

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'sys_get_temp_dir',
            [[]],
            ['tmp']
        );

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'sprintf',
            [[
                '%s/kafka_ca',
                'tmp'
            ]],
            ['tmp/kafka_ca']
        );

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'sprintf',
            [[
                '%s/kafka_user_cert',
                'tmp'
            ]],
            ['tmp/kafka_user_cert']
        );

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'sprintf',
            [[
                '%s/kafka_user_key',
                'tmp'
            ]],
            ['tmp/kafka_user_key']
        );

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'file_put_contents',
            [[
                'tmp/kafka_ca',
                self::KAFKA_CERTIFICATES['kafka_ca']
            ]],
            [1]
        );

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'file_put_contents',
            [[
                'tmp/kafka_user_key',
                self::KAFKA_CERTIFICATES['kafka_user_key']
            ]],
            [1]
        );

        $this->mockMethod(
            KafkaConnectionConfig::class,
            'file_put_contents',
            [[
                'tmp/kafka_user_cert',
                self::KAFKA_CERTIFICATES['kafka_user_cert']
            ]],
            [1]
        );

        $this->reveal();

        $this->getConfig($configContext, $parameterPoolManager)->shouldBeEqualTo([
            'kafka.broker.url' => self::KAFKA_BROKER_URL,
            'kafka.schema.registry.options' => [
                'url' => self::REGISTRY_OPTIONS['url'],
                'username' => self::REGISTRY_OPTIONS['username'],
                'password' => self::REGISTRY_OPTIONS['password'],
            ],
            'kafka.security.options' => [
                'security.protocol' => 'ssl',
                'ssl.ca.location' => 'tmp/kafka_ca',
                'ssl.certificate.location' => 'tmp/kafka_user_cert',
                'ssl.key.location' => 'tmp/kafka_user_key',
            ],
        ]);

        $this->checkPrediction();
    }

    private function mockSchemaRegistryParameters(ParameterPoolManagerInterface $parameterPoolManager): void
    {
        $parameterPoolManager->get('KAFKA_SCHEMA_REGISTRY_URL')->shouldBeCalledOnce()->willReturn(self::REGISTRY_OPTIONS['url']);
        $parameterPoolManager->get('KAFKA_SCHEMA_REGISTRY_USERNAME')->shouldBeCalledOnce()->willReturn(self::REGISTRY_OPTIONS['username']);
        $parameterPoolManager->get('KAFKA_SCHEMA_REGISTRY_PASSWORD')->shouldBeCalledOnce()->willReturn(self::REGISTRY_OPTIONS['password']);
        $parameterPoolManager->get('KAFKA_BROKER_URL')->shouldBeCalledOnce()->willReturn(self::KAFKA_BROKER_URL);
    }

    private function mockKafkaCertificatesParameters(ParameterPoolManagerInterface $parameterPoolManager): void
    {
        $parameterPoolManager->get('KAFKA_CA')->shouldBeCalledOnce()->willReturn(self::KAFKA_CERTIFICATES['kafka_ca']);
        $parameterPoolManager->get('KAFKA_USER_KEY')->shouldBeCalledOnce()->willReturn(self::KAFKA_CERTIFICATES['kafka_user_key']);
        $parameterPoolManager->get('KAFKA_USER_CERT')->shouldBeCalledOnce()->willReturn(self::KAFKA_CERTIFICATES['kafka_user_cert']);
    }
}