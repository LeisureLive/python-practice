class Settings():

    def __init__(self):
        # 游戏屏幕宽度
        self.screen_width = 1200
        # 游戏屏幕高度
        self.screen_height = 800
        # 游戏屏幕背景
        self.bg_color = (230, 230, 230)
        # 飞船速度
        self.ship_speed_factor = 1.5

        # 子弹设置
        self.bullet_speed_factor = 1
        self.bullet_width = 3
        self.bullet_height = 15
        self.bullet_color = 60, 60, 60

        self.alien_speed_factor = 1
        self.fleet_drop_speed = 10
        self.fleet_direction = 1
