function createOpenEventFlowReact(React) {
  const AnalyticsContext = React.createContext(null);

  function OpenEventFlowProvider({ analytics, children }) {
    return React.createElement(AnalyticsContext.Provider, { value: analytics }, children);
  }

  function useAnalytics() {
    const analytics = React.useContext(AnalyticsContext);
    if (!analytics) {
      throw new Error("OpenEventFlowProvider is missing");
    }
    return analytics;
  }

  function useScreen(name, properties = {}) {
    const analytics = useAnalytics();
    React.useEffect(() => {
      analytics.screen(name, properties);
    }, [analytics, name, stableStringify(properties)]);
  }

  function useStay(key, properties = {}, options = {}) {
    const analytics = useAnalytics();
    React.useEffect(() => {
      analytics.beginStay(key, properties);
      return () => analytics.endStay(key, { exitReason: options.exitReason || "component_unmount" });
    }, [analytics, key, stableStringify(properties), stableStringify(options)]);
  }

  function useExposure(ref, eventFactory, options = {}) {
    const analytics = useAnalytics();
    React.useEffect(() => {
      if (!ref.current || typeof IntersectionObserver === "undefined") {
        return undefined;
      }
      let timer = null;
      let tracked = false;
      const threshold = options.threshold || 0.5;
      const durationMs = options.durationMs || 1000;
      const observer = new IntersectionObserver((entries) => {
        for (const entry of entries) {
          if (entry.intersectionRatio >= threshold && !tracked) {
            timer = setTimeout(() => {
              tracked = true;
              analytics.track(eventFactory(entry));
            }, durationMs);
          } else if (timer) {
            clearTimeout(timer);
            timer = null;
          }
        }
      }, { threshold });
      observer.observe(ref.current);
      return () => {
        if (timer) {
          clearTimeout(timer);
        }
        observer.disconnect();
      };
    }, [analytics, ref, eventFactory, options.threshold, options.durationMs]);
  }

  return {
    OpenEventFlowProvider,
    useAnalytics,
    useExposure,
    useStay,
    useScreen
  };
}

function stableStringify(value) {
  return JSON.stringify(value, Object.keys(value).sort());
}

module.exports = {
  createOpenEventFlowReact
};
