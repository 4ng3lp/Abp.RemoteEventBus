using Abp.Reflection;
using System;
using System.Linq;
using System.Collections.Generic;
using Abp.Dependency;
using Abp.Events.Bus;
using Abp.Events.Bus.Handlers;
using Castle.Core.Logging;
using Abp.RemoteEventBus.Exceptions;

namespace Abp.RemoteEventBus.Impl
{
    /// <summary>
    /// Задача этого синглтона при создании сформировать маппинг на основе
    /// зарегистрированных в системе обработчиков сообщений от Кафка.
    /// А при срабатывании метода HandleEvent, определяется тип сообщения и запускается
    /// подходящие обработчики, выбирая из из мапинга
    /// </summary>
    public class AttributeRemoteEventHandler : IEventHandler<RemoteEventArgs>, ISingletonDependency
    {
        public ILogger Logger { get; set; }

        private readonly TypeFinder _typeFinder;

        /// <summary>
        /// Содержит маппинг типов данных в сообщениях от Кафка на типы хендлеров с их атрибутами.
        /// В качестве ключа используется тип данных,
        /// а в качестве значения - список пар: Тип хендлера и его атрибут.
        /// Таким образом на сообщения с одним типом данных может быть зарегистрировано несколько
        /// обработчиков
        /// </summary>
        private readonly Dictionary<string, List<(Type handlerType, RemoteEventHandlerAttribute attribute)>> _typeMapping;

        private readonly IIocResolver _iocResolver;

        private readonly IEventBus _eventBus;

        public AttributeRemoteEventHandler(TypeFinder typeFinder, IIocResolver iocResolver, IEventBus eventBus)
        {
            Logger = NullLogger.Instance;

            _typeFinder = typeFinder;
            _iocResolver = iocResolver;
            _eventBus = eventBus;

            _typeMapping = new Dictionary<string, List<(Type handlerType, RemoteEventHandlerAttribute attribute)>>();

            //Перечень типов-обработчиков, на которые повешаны атрибуты RemoteEventHandlerAttribute
            var typeList = _typeFinder.Find(type =>
                Attribute.IsDefined(type, typeof(RemoteEventHandlerAttribute), false) &&
                typeof(IRemoteEventHandler).IsAssignableFrom(type));

            foreach (var type in typeList)
            {
                var attributes =
                    (RemoteEventHandlerAttribute[]) Attribute.GetCustomAttributes(type,
                        typeof(RemoteEventHandlerAttribute));

                foreach (var attribute in attributes)
                {
                    //обработчик относится к данным определенного типа
                    var key = attribute.ForType;
                    var item = (handlerTyp: type, attribute: attribute);
                    if (_typeMapping.ContainsKey(key))
                    {
                        //если уже в маппинге зарегистрирован тип данных с обработчиками
                        //то  в список добавляется еще один обработчик на тот же ключевой тип
                        var list = _typeMapping[key];
                        list.Add(item);
                    }
                    else
                    {
                        //или же создается новая запись в Dictionary с новым ключем по типу данных
                        _typeMapping.Add(key,
                            new List<(Type handlerType, RemoteEventHandlerAttribute attribute)>(new[] {item}));
                    }
                }
            }
        }

        public void HandleEvent(RemoteEventArgs eventArgs)
            {
                var dataType = eventArgs.EventData.Type;
                //TODO Корректно обработать ситуацию с Key = null
                //Находим в маппинге наборы хендлеров для указанного типа данных в сообщении
                if (dataType != null && _typeMapping.ContainsKey(dataType))
                {
                    //Достаем все хендлеры с соответсвующими им трибутами
                    var handlerPairs = _typeMapping[dataType].OrderBy(p => p.Item2.Order).ToList();

                    //Запускаются все обработчики если они соответсвуют топику сообщения
                    foreach (var handlerPair in handlerPairs)
                    {
                        //Если зарегистрированные хендлеры не подходят под топик сообщения
                        if (handlerPair.Item2.OnlyHandleThisTopic && eventArgs.Topic != handlerPair.Item2.ForTopic)
                        {
                            continue;
                        }

                        try
                        {
                            ///инстанцируется по типу хендлера 
                            var handler = _iocResolver.Resolve<IRemoteEventHandler>(handlerPair.handlerType);
                            handler.HandleEvent(eventArgs);
                        }
                        catch (Exception ex)
                        {
                            Logger.Error("Exception occurred when handle remoteEventArgs", ex);

                            _eventBus.Trigger(this, new RemoteEventHandleExceptionData(ex, eventArgs));

                            if (handlerPair.Item2.SuspendWhenException)
                            {
                                eventArgs.Suspended = true;
                            }
                        }

                        if (eventArgs.Suspended)
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
