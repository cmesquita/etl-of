from datetime import datetime
import json
from pyspark.sql.streaming import StreamingQueryListener

# Define my listener.
class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass
    def onQueryProgress(self, event):
        with open('/dbfs/tmp/data'+datetime.now().strftime("%H%M%S")+'.json', 'w', encoding='utf-8') as f:
            json.dump(event.progress.json, f, ensure_ascii=False, indent=4)
        
    def onQueryTerminated(self, event):
        pass
