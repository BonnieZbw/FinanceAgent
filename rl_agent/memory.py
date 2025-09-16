# in rl_agent/agent.py
from stable_baselines3 import PPO

class CIOAgent:
    def __init__(self, env):
        self.model = PPO("MlpPolicy", env, verbose=1)

    def train(self, total_timesteps=25000):
        self.model.learn(total_timesteps=total_timesteps)

    def predict(self, observation):
        action, _states = self.model.predict(observation)
        return action
# in rl_agent/memory.py
import sqlite3

class EpisodicMemory:
    def __init__(self, db_path='persistence/cio_memory.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS experiences (
                id INTEGER PRIMARY KEY,
                state_text TEXT,
                action REAL,
                reward REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
    def add_experience(self, state_text, action, reward):
        self.cursor.execute(
            "INSERT INTO experiences (state_text, action, reward) VALUES (?, ?, ?)",
            (state_text, float(action), float(reward))
        )
        self.conn.commit()